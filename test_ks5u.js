var _ = require('underscore');
var redis = require('redis').createClient();
var mongoose = require('mongoose');
var request = require('request');

var interval = 1000;
var task_name = 'ks5u';

// 初始化 mongoose，测试和产品的表分开
try {
	mongoose.connect('mongodb://localhost/' + task_name);
	var Item = mongoose.model(task_name, {
		url: {type: String},
		raw: {type: String}
	});
} catch(e) {
	console.log('mongoose 初始化错误。');
	process.exit(1);
}

// 初始化 redis
redis.on("error", function (err) {
    console.log("REDIS 错误: " + err);
});

// 生成所有列表页面并存入 redis 备用
function generate_list() {

	var list_sources = {
		yuwen: {code: 1, total: 94876},
		shuxue: {code: 2, total: 153188},
		yingyu: {code: 5, total: 128580},
		wuli: {code: 9, total: 103359},
		huaxue: {code: 8, total: 100583},
		shengwu: {code: 4, total: 115683},
		zhengzhi: {code: 7, total: 119887},
		lishi: {code: 3, total: 143941},
		dili: {code: 6, total: 76503}
	};

	var per_page = 100;

	_.each(list_sources, function(subject) {

		var subject_pages = _.range(1, Math.ceil(subject.total / per_page) + 1);

		_.each(subject_pages, function(page_index) {
			var page_url = 'http://tiku.ks5u.cn/member.aspx?subjectid=' + subject.code + '&categoryid=&questype=0&quesdiff=0&quesgradeid=0&quesyearid=0&quesaqid=0&quesdiquid=&pageindex=' + page_index + '&pagesize=100&mod=initpaper&ac=list&op=base&inajax=1';
			redis.sadd(task_name + ':waiting', page_url);
		});
	});
}

// 抓取、分析并保存结果
function consume(url) {


	var request_options = {
		url: url,
		headers: {
			'Referer': 'http://tiku.ks5u.cn/',
			'User-Agent': 'Mozilla/5.0 (Windows NT 5.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/32.0.1700.76 Safari/537.36'
			},
		timeout: 20000
	};

	request(request_options, function(err, res, buf) {
	
		redis.hdel(task_name + ':running', url);

		if (err) {
			redis.sadd(task_name + ':crawl_data_error', url);
			console.log('读取错误 -> %s -> %s', url, err);
		} else {
			if (res.statusCode == 200) {
				// 正常
				var item_data = {};

				try {
					var clean = buf.replace(/<(?!img|a).*?>/ig, '');
					clean = clean.replace(/\&nbsp;/ig, '');
				} catch(e) {
					redis.sadd(task_name + 'clean_data_error', url);
				}

				item_data.url = url;
				item_data.raw = clean;

				var item = new Item(item_data);
				item.save(function(err, doc) {
					if (err) {
						redis.sadd(task_name + ':save_data_error', url);
					} else {
						redis.incr(task_name + ':item_success_count');
					}
				});

			} else {
				// 不正常
				redis.sadd(task_name + ':crawl_data_not200', url + ':' + res.statusCode);
			}
		}
	});
}

function start_consume() {

	var timer = setInterval(function() {

		redis.spop(task_name + ':waiting', function(err, waiting_url) {
			redis.hlen(task_name + ':running', function(err, running_count) {
				if (waiting_url == null) {
					if (running_count <= 0) {
						console.log('等待队列已空，等待完成');
						clearInterval(timer);
					} else {
						// 队列已空，等待完成
					}
				} else {
					redis.hset(task_name + ':running', waiting_url, '1');
					consume(waiting_url);
				}
			});
		});
	}, interval);
}

function clear_data() {
	// 擦除 redis 数据
	redis.del(
		task_name + ':waiting',
		task_name + ':running',
		task_name + ':item_success_count',
		task_name + ':crawl_data_error',
		task_name + ':crawl_data_not200',
		task_name + ':clean_data_error',
		task_name + ':save_data_error'
	);

	// 擦除 mongodb 数据
	Item.remove({}, function(err) {
		if (err) console.log('Mongodb 清空数据出错');
	});
}

// 生成页面列表
generate_list();
console.log('程序将在 3 秒后正式开始');
// 3 秒钟后运行
setTimeout(function() {
	redis.scard(task_name + ':waiting', function(err, reply) {
		console.log('开始处理 %s 条任务', reply);
		start_consume();
	});
}, 3000);