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

	// 15 秒时间人工更新认证码后尝试登录
	setTimeout(function() {
		var request_entrance_options = request_options;

		request_entrance_options.url = 'http://113.105.70.79:8000/auth/login.do';
		request_entrance_options.headers.Referer = 'http://113.105.70.79:8000/authjsp/login.jsp';
		request_entrance_options.form = {
			url: '',
			mac: '',
			network: '113.105.70.79',
			username: '0440050145638',
			password: '58e6da98fbc635e5a0962a4732e12084',
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

				var total = Math.ceil(91631 / 10);
				// var total = 3;
				var current = 0;

				var timer = setInterval(function() {
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
		
			});
		});

	}, 15000);
});

function a340(u,p){
	Dotfuse.init(u,p);
	return Dotfuse.f340();
}