var _ = require('underscore');
var fs = require('fs');
var redis = require('redis').createClient();
var request = require('request');
var cheerio = require('cheerio');
var Dotfuse = require('./bin/dotfuse');

// 初始化 redis
redis.on("error", function (err) {
    console.log("REDIS 错误: " + err);
});

var request = request.defaults({jar: true});

var done = 0;

redis.hkeys('vper:running', function(jobs) {
	redis.sadd('vper:waiting', jobs, function(err, reply) {
		console.log('%s 条之前未完成的任务已放回等待列表', reply);
	});
});

//读取首页自动获取 cookie
request('http://192.168.101.18:8083/UI/index.aspx', function(err, res, buf) {

	if (err) {
		console.log('错误：入口页 -> http://192.168.101.18:8083/UI/index.aspx');
		process.exit(1);
	}
	// 请求搜索首页获取隐藏数据用于提交查询
	request('http://192.168.101.18:8083/UI/Search.aspx?curr=3', function(err, res, buf) {
		
		if (err) {
			console.log('错误：搜索首页 -> http://192.168.101.18:8083/UI/Search.aspx?curr=3');
			process.exit(1);
		}

		var $ = cheerio.load(buf.toString());

		// 定义查询表单内容
		var options = {
			url: 'http://192.168.101.18:8083/UI/Search.aspx?curr=3',
			method: 'POST',
			headers: {
				'Referer': '',
				'User-Agent': 'Mozilla/5.0 (Windows NT 5.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/32.0.1700.76 Safari/537.36'
				},
			form: {
				__VIEWSTATE: $('input#__VIEWSTATE').val(),
				__EVENTVALIDATION: $('input#__EVENTVALIDATION').val(),
				Web_login1$txtUserName: '',
				Web_login1$txtPassWord: '',
				hKClassId: -1,
				Param_StartYear: -1,
				Param_EndYear: -1,
				txtSearch: '',
				ckl_Search: 2,
				hid: 1,
				hpage: 0,
				nPage: 1
			},
			timeout: 20000
		};

		console.log(options);
		console.log('正式开始。\n');

		var timer = setInterval(function() {

			redis.spop('vper:waiting', function(err, page) {
				if (parseInt(page) > 0) {
					var request_options = options;
					request_options.form.hpage = parseInt(page) - 1;
					request_options.form.nPage = parseInt(page);
					consume(request_options);
				} else {
					console.log('等待队列已空，即将结束。');
				}			
			});

		}, 3000);

	});
});

function consume(options) {

	var page = options.form.nPage;
	redis.hset('vper:running', page, 1);

	request(options, function(err, res, buf) {

		if (err) {
			console.log('抓取错误 -> 第 %d 页 -> %s', page, err);
			redis.sadd('vper:waiting', page, function(err, reply) {
				console.log('已经第 %d 页放回等待列表 - %s', page, reply);
				redis.incr('vper:crawl_error');
			});
		} else {
			try {
				var body = buf.toString();
				var exams = body.match(/ExamId=[\d\w]+/ig);
				if (exams.length > 0) {
					redis.sadd('vper:exam_id', exams, function(err, reply) {
						done += 1;
						console.log('%d - 成功 -> 第 %d 页 -> 获取 %d 条结果', done, page, exams.length);
					});
				} else {
					console.log('解析结果为空 -> 第 %d 页', page);
					redis.sadd('vper:waiting', page);
					redis.incr('vper:parse_empty');
				}

			} catch(e) {
				console.log('解析错误 -> 第 %d 页aaa -> %s', page, e);
				//redis.sadd('vper:waiting', page);
				redis.incr('vper:parse_error');
			}
		}

		redis.hdel('vper:running', page);
	});
}