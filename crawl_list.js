/**
 * node crawl start 自动判断是全新或继续上一次的执行
 * node crawl clean 清空执行数据，直接退出
 * node crawl debug 测试运行，缺省测试条目为 3
 * node crawl stats 显示上一次运行数据
 * node crawl gtask 仅生成任务清单，不执行任务
 */

var _ = require('underscore');
var fs = require('fs');
var redis = require('redis').createClient();
var mongoose = require('mongoose');
var request = require('request');
var cheerio = require('cheerio');
var nodemailer = require('nodemailer');
// 需要导入的其他功能库
var Dotfuse = require('./bin/dotfuse');

// 程序配置
var task_name = 'vper_list';
var init_url = 'http://113.105.70.79:8000/';
var crawl_timer;
var crawl_interval = 3000;
var send_info_timer;
var send_info_interval = 1000 * 60 * 30;
var check_end_interval = 1000;
var debug_count = 3;
var crawl_error_end_count = 2000;
var parse_error_end_count = 2000;
// 如果需要先登录，配置以下信息
var need_login_first = false;
if (need_login_first) {
	var login_url = '';
	var login_options = {
		url: login_url,
		method: 'POST',
		headers: {
			'Referer': login_url,
			'User-Agent': 'Mozilla/5.0 (Windows NT 5.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/32.0.1700.76 Safari/537.36'
			},
		form: {
			// 表单数据根据需要修改
			username: '',
			password: '',
			verifycode: ''
		},
		timeout: 20000
	};
	var login_success_flag = '';
	var need_verifycode = false;
	if (need_verifycode) {
		var verifycode_url = '';
		var verifycode_ext = '.jpg'; // 验证码的格式，如：.jpg | .png | .gif 等
	}
}
// 从命令行获取运行模式，如果没有指定运行模式，直接退出
if ( _.indexOf(['start', 'clean', 'gtask', 'debug', 'stats'], process.argv[2]) < 0) {
	console.log('请指定参数\n  start - 全新或继续开始\n  debug - 试运行 %d 条数据\n  gtask - 仅生成任务数据\n  clean - 清除数据\n  stats - 显示上一次运行情况。', debug_count);
	process.exit(1);
} else {
	var run_mode = process.argv[2];
	var rdb_prefix = task_name;
	if (run_mode == 'debug') rdb_prefix = task_name + '_debug';
	var rfs = {
		waiting: rdb_prefix + ':waiting',
		running: rdb_prefix + ':running',
		success: rdb_prefix + ':success',
		crawl_error: rdb_prefix + ':crawl_error',
		parse_error: rdb_prefix + ':parse_error',
		item: rdb_prefix + ':item'
	};
}

// 初始化 redis
redis.on("error", function (err) {
    console.log("REDIS 错误: " + err);
});
// 初始化 mongodb

// 初始化 request
var request = request.defaults({jar: true});
// 初始化邮件发送设置
try {
	var mail = nodemailer.createTransport('SMTP', {
		service: 'QQ',
		auth: {
			user: '2214431392@qq.com',
			pass: 'zhaomeng'
		}
	});
} catch(e) {
	console.log('NodeMailer 初始化错误。');
	process.exit(1);
}

// 如果是 stats，显示统计数据，直接退出
if (run_mode == 'stats') {
	show_stats_info(run_mode);
	process.exit(0);
}

// 如果是 clean，清空 redis 数据，直接退出
// 1、clean || clean status - 删除数据，但保留 item 项
// 2、clean all - 删除所有数据
if (run_mode == 'clean') {
	var clean_mode = process.argv[3] || 'status';

	if (clean_mode == 'all') {
		redis.del(rfs.waiting, rfs.running, rfs.success, rfs.crawl_error, rfs.parse_error, rfs.item, function(err, reply) {
			console.log('所有数据清除完毕，包括 item 在内的数据已全部清空。');
			process.exit(0);
		});
	} else {
		redis.del(rfs.waiting, rfs.running, rfs.success, rfs.crawl_error, rfs.parse_error, function(err, reply) {
			console.log('所有数据清除完毕，但保留了 item 项。如需删除该项数据，请使用 clean all 命令。');
			process.exit(0);
		});
	}
}

// 如果是 gtask，生成任务数据，直接退出
if (run_mode == 'gtask') {
	// 直接退出已定义在下面的函数中
	// 不用再 process.exit() 了
	generate_task();
}

// 如果是 debug，清除数据，以 3 条数据填充
// 由于 debug 模式下使用单独的数据库，所以放心删除所有数据
if (run_mode == 'debug') {
	redis.del(rfs.waiting, rfs.running, rfs.success, rfs.crawl_error, rfs.parse_error, rfs.item, function(err, reply) {
		// 填充 waiting 数据
		generate_task();
		// 开始执行，因为填充数据需要时间，等待 2000 毫秒
		setTimeout(start_consume, 2000);
	});
}

// 如果是 start，检查是全新还是继续
// 如果 waiting 或 running 不为空，则表示上次已经运行过
// 如果 waiting, running 均为空，则分两种情况：
// 1、如果 success 不为空，则判断为上次已运行，且已完成所有任务，程序退出
// 2、如果 success 也是空，则判断为任务从未运行，应为全新开始，填充 waiting 数据
if (run_mode == 'start') {

	redis.multi()
		.scard(rfs.waiting)
		.hlen(rfs.running)
		.get(rfs.success)
		.exec(function(err, replies) {
			var waiting_count = parseInt(replies[0]);
			var running_count = parseInt(replies[1]);
			var success_count = parseInt(replies[2]);

			if (waiting_count > 0 || running_count > 0) {
				// 上次未完成，继续
				if (running_count > 0) {
					// 上次有已开始但未完成的任务，先将其放回 waiting 列表
					redis.hkeys(rfs.running, function(err, running_jobs) {
						redis.sadd(rfs.waiting, running_jobs, function(err, reply) {
							console.log('发现以前曾经运行但未完成的任务 %d 条，已放回任务列表。', running_jobs.length);
							redis.scard(rfs.waiting, function(err, reply) {
								console.log('任务总量：%s，开始执行。', reply);
								start_consume();
							});
						});
					});
				} else {
					// 不存在已运行但未完成的任务，直接继续开始
					redis.scard(rfs.waiting, function(err, reply) {
						console.log('任务总量：%s，开始执行。', reply);
						start_consume();
					});
				}
			} else {
				// waiting 和 running 都为空
				if (success_count > 0) {
					// 如果 success 不为空，则表示上次执行已完成
					console.log('上次的任务已执行并完成，程序退出。');
					console.log('如确需重新开始该项目，请先使用 clean 命令清除程序数据。');
					console.log('清除数据前，请确保已检查并备份重要数据！');
					process.exit(0);
				} else {
					// 如果 success 也为空，则可判断为全新执行
					// 删除数据，填充全部数据
					generate_task();
					// 开始执行，因为填充数据需要时间，等待 3000 毫秒
					setTimeout(start_consume, 3000);
				}
			}

		});

}

/**
 * start_consume - 执行任务主逻辑
 * 1、每隔 crawl_interval 执行一条任务
 * 2、每隔 send_mail_interval 发送一封状态信息
 * 3、每隔 check_end_interval 检查任务是否已完成
 */
function start_consume() {

	request(init_url, function(err, reply) {

		if (err) {
			console.log('访问任务入口错误，程序退出。');
			process.exit(1);
		}

		setInterval(check_end, check_end_interval);

		if (need_login_first) {
			// 需要先登录
			if (need_verifycode) {
				// 如果需要验证码
				request(verifycode_url).pipe(fs.createWriteStream('verifycode' + verifycode_ext));
				console.log('请在 50 秒内将 verifycode.%s 中的文字保存到 verifycode.txt ......', verifycode_ext);
			}

			setTimeout(function() {
				// 等待 55 秒后开始尝试登录
				if (need_verifycode) login_options.form.verifycode = fs.readFileSync('verifycode.txt').toString();
				request(login_options, function(err, res, buf) {
					var content = buf.toString();
					// 测试是否成功，如果成功则继续，不成功则直接退出
				});
			}, 55 * 1000);
		} else {
			// 不需要先登录
			crawl_timer = setInterval(consume, crawl_interval);
			send_info_timer = setInterval(show_stats_info, send_info_interval);
		}
	});
}

function consume() {

	redis.spop(rfs.waiting, function(err, task) {

		if (task) {
			// 将任务放入 running
			redis.hset(rfs.running, task, 1);
			// 配置 request 选项
			var request_options = {
				url: '',
				method: 'GET',
				headers: {
					'Referer': '',
					'User-Agent': 'Mozilla/5.0 (Windows NT 5.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/32.0.1700.76 Safari/537.36'
					},
				form: {

				},
				timeout: 20 * 1000
			};
			// 抓取数据
			request(request_options, function(err, res, buf) {
				if (err) {
					// 如果抓取错误，将该任务放回 waiting，同时 crawl_error + 1
					console.log('抓取错误 -> %s -> %s -> %s', task, request_options.url, err);
					redis.sadd(rfs.waiting, task);
					redis.incr(crawl_error);
				} else {
					// 如果抓取成功，则开始分析和保存数据
					parse_and_save(buf.toString(), task);
				}
				// 不论是否抓取成功，都应将该任务从 running 中删除
				redis.hdel(rfs.running, task);
			});
		}

	});
}

function parse_and_save(content, task) {

	// 如果解析错误，则将任务放回 waiting，同时 parse_error + 1

}

function save(data) {

	// 可以保存在 redis, mongo, file 等
	// 如果保存错误，则将任务放回 waiting，同时 save_error + 1
	// 如果保存成功，success + 1

}

function check_end() {
	redis.multi()
		.scard(rfs.waiting)
		.hlen(rfs.running)
		.get(rfs.crawl_error)
		.get(rfs.parse_error)
		.exec(function(err, replies) {
			var waiting_count = parseInt(replies[0]);
			var running_count = parseInt(replies[1]);
			var crawl_error_count = parseInt(replies[2]);
			var parse_error_count = parseInt(replies[3]);

			if (waiting_count <= 0 && running_count <= 0) {
				console.log('所有任务已完成，为保证后台数据有足够时间存储，程序将等待 60 秒后退出。');
				clearInterval(crawl_timer);
				clearInterval(send_info_timer);
				send_mail(task_name + ' All Tasks Completed', '任务完成！');
				setTimeout(function() {
					console.log('所有任务已完成，程序退出。');
					process.exit(0);
				}, 60 * 1000);
			}

			if (crawl_error_count >= crawl_error_end_count) {
				console.log('抓取错误次数超过 %d 次，程序退出。', crawl_error_end_count);
				clearInterval(crawl_timer);
				clearInterval(send_info_timer);
				send_mail(task_name + ' 抓取错误次数异常，程序退出。', '抓取错误次数超限！');
				setTimeout(function() {
					process.exit(0);
				}, 10 * 1000);
			}

			if (parse_error_count >= parse_error_end_count) {
				console.log('数据解析错误次数超过 %d 次，程序退出。', parse_error_end_count);
				clearInterval(crawl_timer);
				clearInterval(send_info_timer);
				send_mail(task_name + ' 数据解析错误次数异常，程序退出。', '数据解析错误次数超限！');
				setTimeout(function() {
					process.exit(0);
				}, 10 * 1000);
			}
		});
}

/**
 * generate_task - 生成任务数据
 * 
 * 如果运行模式为 debug，则生成 debug_count 条数据，否则生成全部数据
 * 所有数据存入 rfs.waiting 中
 * 
 * @param  {String} run_mode 程序运行模式
 */
function generate_task() {

	var tasks = [];

	if (run_mode == 'debug') {
		// 生成测试数据，数量由 debug_count 定义
	} else {
		// 生成所有任务数据
		// 如果运行模式为 gtask，生成后直接退出
	}
}

/**
 * show_stats_info - 显示或发送统计信息
 * @param  {String} run_mode 运行模式，可以是 start, debug, clean, stats
 */
function show_stats_info() {

	var info = '';

	redis.multi()
		.scard(rfs.waiting)
		.hlen(rfs.running)
		.get(rfs.success)
		.get(rfs.crawl_error)
		.get(rfs.parse_error)
		.scard(rfs.item)
		.exec(function(err, replies) {

			info += '等待：' + replies[0] + ' ';
			info += '运行：' + replies[1] + ' ';
			info += '成功：' + replies[2] + ' ';
			info += '抓取错误：' + replies[3] + ' ';
			info += '解析错误：' + replies[4] + ' ';
			info += '已保存：' + replies[5];

			if (run_mode == 'start') {
				send_mail(task_name + ' Status Report', info);
			} else {
				console.log(info);
			}

		});
}

/**
 * send_mail - 发送邮件
 * @param  {String} subject 邮件标题
 * @param  {String} body    邮件正文
 */
function send_mail(subject, body) {
	var mail_options = {
		from: 'zwm - ' + task_name + ' <2214431392@qq.com>',
		to: '2214431392@qq.com',
		subject: subject,
		text: body
	};

	mail.sendMail(mail_options, function(err, res) {
		if (err) {
			console.log('邮件 - 发送错误 -> %s', err);
		} else {
			console.log('邮件 - 发送成功 -> %s', res.message);
		}
	});
}