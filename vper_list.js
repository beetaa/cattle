/**
 * node crawl_list start 自动判断是全新或继续上一次的执行
 * node crawl_list clean 清空执行数据，直接退出
 * node crawl_list debug 测试运行，缺省测试条目为 3
 * node crawl_list stats 显示上一次运行数据
 */

var _ = require('underscore');
var fs = require('fs');
var redis = require('redis').createClient();
var mongoose = require('mongoose');
var request = require('request');
var cheerio = require('cheerio');
var Dotfuse = require('./bin/dotfuse');

// 从命令行获取运行模式，如果没有指定运行模式，直接退出
if ( _.indexOf(['start', 'clean', 'debug', 'stats'], process.argv[2]) < 0 ) {
	console.log('请指定参数，start 全新或继续开始，debug 试运行 3 条数据，clean 清除数据，stats 显示上一次运行情况。');
	process.exit(1);
}

var run_mode = process.argv[2];
var debug_count = 3;
var task_name = 'vper';

// 初始化 redis
redis.on("error", function (err) {
    console.log("REDIS 错误: " + err);
});


// 

// 要运行的所有页面
var total = Math.ceil(91631 / 10);
var run_total = (run_mode == 'debug') ? 3 : total;

// 如果是 stats，显示上一次运行情况，直接退出
if (run_mode == 'stats') {
	redis.multi()
		.scard('vper:waiting')
		.hlen('vper:running')
		.scard('vper:exam_id')
		.get('vper:crawl_error')
		.get('vper:parse_error')
		.get('vper:success')
		.exec(function(err, replies) {
			console.log('stats mode -> 等待：%s.', replies[0]);
			console.log('stats mode -> 运行：%s.', replies[1]);
			console.log('stats mode -> 已抓取项目：%s.', replies[2]);
			console.log('stats mode -> 抓取错误：%s.', replies[3]);
			console.log('stats mode -> 数据错误：%s.', replies[4]);
			console.log('stats mode -> 成功：%s.', replies[5]);
			process.exit(0);
		});
}

// 如果是 clean，清空 redis 数据，直接退出
// 如果是 debug，清除数据，以 3 条数据填充，不退出
// 如果是 start，检查是全新还是继续，如果是全新，则填充数据
if (run_mode == 'clean' || run_mode == 'debug') {
	redis.del('vper:waiting', 'vper:running', 'vper:crawl_error', 'vper:parse_error', 'vper:success', function(err, reply) {
		if (run_mode == 'clean') {
			console.log('clean mode -> 数据清除完毕，退出。');
			process.exit(0);
		} else {
			console.log('debug mode -> 数据清除完毕。');
			_.each(_.sample(total, run_total), function(page) {
				redis.sadd('vper:waiting', page);
			});
			redis.scard('vper:waiting', function(err, reply) {
				console.log('debug mode -> 有 %s 条任务需要处理。', reply);
			});
		}
	});
} else {
	// start mode, 判断全新还是继续
	redis.scard('vper:waiting', function(err, reply) {
		var waiting_count = parseInt(reply);
		redis.hlen('vper:running', function(err, reply) {
			var running_count = parseInt(reply);

			if (waiting_count <= 0 && running_count <= 0) {
				console.log('start mode -> 上次任务已完成，退出。')；
				process.exit(0);
			} else {
				if (running_count > 0) {
					redis.hkeys('vper:running', function(err, running_jobs) {
						redis.sadd('vper:waiting', running_jobs, function(err, reply) {
							console.log('start mode -> 前一次已运行但未完成的 %d 条任务已放回等待列表。', running_jobs.length);
							redis.scard('vper:waiting', function(err, reply) {
								console.log('start mode -> 有 %s 条任务需要处理。', reply);
							});
						});
					});
				}
			}
		});
	});

}

// 初始化 request
var request = request.defaults({jar: true});
var request_options = {
	url: '',
	method: 'POST',
	headers: {
		'Referer': '',
		'User-Agent': 'Mozilla/5.0 (Windows NT 5.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/32.0.1700.76 Safari/537.36'
		},
	form: {},
	timeout: 20000
};


//读取首页自动获取 cookie
request('http://113.105.70.79:8000/', function(err, res, buf) {

	// 读取验证码，保存为文件
	request('http://113.105.70.79:8000/servlet/verifycode').pipe(fs.createWriteStream('verifycode.jpg'));

	// 30 秒时间人工更新认证码后尝试登录
	setTimeout(function() {
		var request_entrance_options = request_options;

		request_entrance_options.url = 'http://113.105.70.79:8000/auth/login.do';
		request_entrance_options.headers.Referer = 'http://113.105.70.79:8000/authjsp/login.jsp';
		request_entrance_options.form = {
			url: '',
			mac: '',
			network: '113.105.70.79',
			username: '0440050145638',
			password: a340('0440050145638', '19770604'),
			verifycode: fs.readFileSync('verifycode.txt').toString()
		};

		// 尝试登录
		request(request_entrance_options, function(err, res, buf) {

			request('http://113.105.70.79:8000/rewriter/VERS/http/udqr9bpuho9bnl/UI/Search.aspx?curr=3', function(err, res, buf) {

				var $ = cheerio.load(buf.toString());
				var request_search_options = request_options;
				var item_url_pattern = /ExamId=[\d\w]+/ig;

				request_search_options.url = 'http://113.105.70.79:8000/rewriter/VERS/http/udqr9bpuho9bnl/UI/Search.aspx?curr=3';
				request_search_options.form = {
					__VIEWSTATE: $('input#__VIEWSTATE').val(),
					__EVENTVALIDATION: $('input#__EVENTVALIDATION').val(),
					Web_login1$txtUserName: '0440050145638',
					Web_login1$txtPassWord: '19770604',
					hKClassId: -1,
					Param_StartYear: -1,
					Param_EndYear: -1,
					txtSearch: '',
					ckl_Search: 2,
					hid: 1,
					hpage: 0,
					nPage: 1
				};

				start_consume(request_search_options);
		
			});
		});

	}, 30000);
});

function a340(u,p){
	Dotfuse.init(u,p);
	return Dotfuse.f340();
}

function start_consume(options) {

	var search_options = options;

	var timer = setInterval(function() {

		redis.scard('vper:waiting', function(err, reply) {
			var waiting_count = parseInt(reply);
			if (waiting_count > 0) {
				redis.spop('vper:waiting', function(err, reply) {

				});
			} else {
				redis.hlen('vper:running', function(err, reply) {
					var running_count = parseInt(reply);
					if (running_count <= 0) {
						console.log('所有任务已完成，可以退出。');
						clearInterval(timer);
					}
				});
			}
		});

		if (current > total) {
			clearInterval(timer);
		} else {
			current += 1;
			request_search_options.form.hpage = current - 1;
			request_search_options.form.nPage = current;

			request(request_search_options, function(err, res, buf) {
				if (err) {
					console.log('错误：第 %s 页 -> %s', current, err);
					redis.sadd('vper:error_page', current);
				} else {
					console.log('抓取第 %d 页，状态码：%s，信息长度：%s 字节', current, res.statusCode, buf.length);
					var txt = buf.toString();
					var exams = txt.match(item_url_pattern);
					_.each(exams, function(exam_id) {
						redis.sadd('vper:exam_id', exam_id);
					});
				}
			});
		}
	}, 1000);

}