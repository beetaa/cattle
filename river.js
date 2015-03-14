var _ = require('underscore');
var redis = require('redis').createClient();
var mongoose = require('mongoose');
var request = require('request');
var cheerio = require('cheerio');
var nodemailer = require('nodemailer');

// 处理命令行参数，初始化 job 配置。
if (!process.argv[2]) {
	console.log('未指定 job 配置文件。请重新运行。');
	process.exit(1);
} else {
	var job_name = process.argv[2];
	try {
		var job = require('./job/' + job_name);
	} catch(e) {
		console.log('不存在此配置文件: ' + job_name);
		process.exit(1);
	}
}

// 从命令行获取运行模式。
var run_mode = process.argv[3] || 'none';

// 初始化 mongoose，测试和产品的表分开
try {
	mongoose.connect('mongodb://localhost/' + job.db_name);
	var item_table_name = (job.debug || run_mode == 'debug') ? job.item_table_name + '_debug' : job.item_table_name;
	var Item = mongoose.model(item_table_name, job.item_table_schema);
} catch(e) {
	console.log('mongoose 初始化错误。');
	process.exit(1);
}

// 初始化 redis
redis.on("error", function (err) {
    console.log("REDIS 错误: " + err);
});

// 初始化邮件发送设置
try {
	var mail = nodemailer.createTransport('SMTP', job.smtp_options);
} catch(e) {
	console.log('nodemailer 初始化错误。');
	process.exit(1);
}

function consume(url) {

	// 初始化 request 参数
	var options = job.request_options;
	options.url = url;

	// 开始抓取数据并处理
	request(options, function(error, response, buffer) {

		if (error) {
			// 抓取数据发生错误，将 url 放回去
			redis.sadd(job.task_name + ':waiting', url);
			redis.hdel(job.task_name + ':running', url);
			redis.incr(job.task_name + ':crawl_data_error_count');
			console.log('读取错误 -> %s -> %s', url, error);
		} else {
			// 数据成功抓取，分情况处理
			if (job.debug || run_mode == 'debug') console.log('读取状态 - %d -> %s', response.statusCode, url);

			if (response.statusCode == 200) {
				// 返回码 200，正常
				if (job.is_list_page(url)) {
					// 如果是列表页，则解析出item url并推入redis队列
					var item_urls = job.discover_item_urls(buffer.toString());
					if (job.debug) console.log(item_urls);
					if (!item_urls) {
						// 数据提取发生异常
						redis.sadd(job.task_name + ':refine_data_error_set', url);
					} else {
						// 数据提取正常，就新发现的 item 加入 waiting
						_.each(item_urls, function(item_url) {
							redis.sadd(job.task_name + ':waiting', item_url);
						});
					}
				} else {
					// 如果是具体项目页，则解析出具体数据
					var item_data = job.refine_item_data(buffer.toString());
					if (!item_data) {
						// 数据提取发生异常
						redis.sadd(job.task_name + ':refine_data_error_set', url);
					} else {
						try {
							// 如果数据提取正常，填充额外信息
							item_data.url = url;
							// 将数据保存到 mongodb
							var item = new Item(item_data);
							item.save(function(err, doc) {
								if (err) {
									// 如果保存数据发生错误，记录错误页
									redis.sadd(job.task_name + ':save_data_error_set', url);
								} else {
									// 如果数据保存成功，更新统计数据
									redis.incr(job.task_name + ':item_success_count');
								}
							});
						} catch(e) {
							console.log('MONGOOSE - 保存数据时发生错误。');
						}
					}
				}
			} else {
				// 返回码非 200，异常
				redis.hset(job.task_name + ':crawl_data_not200_hash', url, String(response.statusCode));
			}
		}

		redis.hdel(job.task_name + ':running', url);
		
	});
}

function start_consume() {

	var timer = setInterval(function() {

		redis.spop(job.task_name + ':waiting', function(err, url) {
			redis.hlen(job.task_name + ':running', function(err, running_count) {
				if (url === null) {
					// 如果队列已空
					if (running_count <= 0) {
						// 队列已空，且没有任务在运行，可以退出
						console.log('PROCESS - 等待队列已无任务，程序正在退出...');
						redis.del(job.task_name + ':feed_list_ok');
						send_status('River Job - ' + job.task_name + ' Completed.');
						clearInterval(timer);
						setTimeout(function() {
							process.exit(0);
						}, 10000);
					} else {
						// console.log('队列已空，但尚有任务未完成，等待结束中...');
					}
				} else {
					// console.log('处理 -> %s', url);
					redis.hset(job.task_name + ':running', url, '1');
					consume(url);
				}
			});
		});

	}, job.interval);

	var send_mail_timer = setInterval(function() {

		send_status('River Status Report');

	}, job.send_mail_interval);

}

function clear_redis_data() {

	redis.del(
		job.task_name + ':feed_list_ok',  // 是否已完成填充 list 页面，真值为 'ok'

		job.task_name + ':waiting',  // 等待运行的任务，set
		job.task_name + ':running',  // 正在运行的任务，hash，已 url 为键，方便删除

		job.task_name + ':crawl_data_not200_hash',
		job.task_name + ':refine_data_error_set',
		job.task_name + ':save_data_error_set',

		job.task_name + ':item_success_count',  // 保存成功的任务，仅限于 item
		job.task_name + ':crawl_data_error_count',
		job.task_name + ':send_mail_error_count'
	);
}

function send_status(subject) {

	try {
		redis.multi()
			.scard(job.task_name + ':waiting')
			.hlen(job.task_name + ':running')
			.get(job.task_name + ':item_success_count')
			.get(job.task_name + ':crawl_data_error_count')
			.exec(function(err, replies) {

				var content = '未开始：' + replies[0] + '。';
				content += '进行中：' + replies[1] + '。';
				content += '成功保存：' + replies[2] + '。';
				content += '抓取失败：' + replies[3] + '。';

				var mailOptions = {
					from: 'zwm - ' + job.task_name + ' <2214431392@qq.com>',
					to: '2214431392@qq.com',
					subject: subject,
					text: content
				};

				mail.sendMail(mailOptions, function(err, res) {

					if (err) {

						console.log('MAIL - 发送错误 -> %s', err);
						redis.incr(job.task_name + ':send_mail_error_count');
					} else {

						console.log('MAIL - 发送成功 -> %s', res.message);
					}
				});

			});
	} catch(e) {

		console.log('发送邮件错误。');
	}
}

function run() {

	if (job.debug || run_mode == 'debug') {
		// 测试模式，擦除数据，抽取样本全新执行
		console.log('PROCESS - 测试模式，重新初始化。');
		clear_redis_data();
		var list_urls = job.generate_list_urls();
		list_urls = _.sample(list_urls, job.debug_list_page_count);
		_.each(list_urls, function(url) {
			redis.sadd(job.task_name + ':waiting', url);
		});
		redis.set(job.task_name + ':feed_list_ok', 'ok');
		Item.remove({}, function(err) {
			if (err) {
				console.log('MONGOOSE - 擦除测试数据库时错误。程序退出。');
				process.exit(1);
			}
			// 设定 1000 毫秒后才开始真正执行
			setTimeout(function() {
				console.log('PROCESS - 测试模式初始化完成。有 %d 条任务需要处理。', list_urls.length);

				// 此处放置处理代码
				start_consume();
				
			}, 1000);
		});
	} else {
		// 产品模式
		console.log('PROCESS - 产品模式，检测任务状态。');
		redis.get(job.task_name + ':feed_list_ok', function(err, feed_list_ok) {
			if (feed_list_ok == 'ok' && run_mode != 'restart') {
				// 程序曾经初始化，检测是否已经完成
				redis.scard(job.task_name + ':waiting', function(err, waiting_count) {
					redis.hlen(job.task_name + ':running', function(err, running_count) {
						if (parseInt(waiting_count) > 0 || parseInt(running_count > 0)) {
							console.log('PROCESS - 上次的任务尚未完成。有 %s 条任务从未获得处理。', waiting_count);
							console.log('PROCESS - 上次有 %s 条任务已开始但未完成。转存至 waiting 列表重新处理。', running_count);
							// 如果存在上次已运行但未完成的任务，将其放回 waiting 列表
							// 这里的处理方法有问题，如果另一线程开始，则这里会错乱！
							if (parseInt(running_count) > 0) {
								redis.hkeys(job.task_name + ':running', function(err, running_urls) {
									_.each(running_urls, function(running_url) {
										redis.sadd(job.task_name + ':waiting', running_url);
									});
								});
							}
							// 删除上次的 running 数据
							redis.del(job.task_name + ':running');
							// 设定 30000 毫秒后开始真正执行任务
							setTimeout(function() {
								start_consume();
							}, 3000);

						} else {
							console.log('PROCESS - 上次的任务已执行完毕，程序正常退出。');
							process.exit(0);
						}

					});
				});
			} else {
				// 程序尚未初始化过，或者在参数中指定 restart，全新开始
				console.log('PROCESS - 任务全新开始 ...');
				console.log('PROCESS - 初始化 Redis 数据 ...');
				clear_redis_data();
				console.log('PROCESS - 生成、写入列表页数据 ...');
				var list_urls = job.generate_list_urls();
				_.each(list_urls, function(url) {
					redis.sadd(job.task_name + ':waiting', url);
				});
				redis.set(job.task_name + ':feed_list_ok', 'ok');
				console.log('PROCESS - 清空 MongoDB 数据 ...');
				Item.remove({}, function(err) {
					if (err) {
						console.log('MONGOOSE - 擦除产品数据时发生错误。程序退出。');
						process.exit(1);
					}
					// 设定 3000 毫秒后开始真正执行任务
					setTimeout(function() {
						console.log('PROCESS - 产品模式初始化完成。有 %d 条任务需要处理。', list_urls.length);

						// 此处放置处理代码
						start_consume();
						
					}, 3000);
				})
				
			}
		})

	}
}

run();