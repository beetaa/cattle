var _ = require('underscore');
var fs = require('fs');
var cheerio = require('cheerio');
var request = require('request');

request('http://www.baidu.com/img/bdlogo.gif')
	.on('end', function() {
		console.log('请求完成');
	})
	.on('error', function(err) {
		console.log('抓取错误 -> ' + err);
	})
	.on('response', function(res) {
		console.log(res.statusCode);
	})
	.pipe(fs.createWriteStream('baidu/bdlogo.gif'))
	.on('close', function() {
		console.log(fs.statSync('baidu/bdlogo.gif'));
	})
	.on('error', function(err) {
		console.log('保存错误 -> ' + err);
	});