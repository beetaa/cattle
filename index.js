var _ = require('underscore');
//var redis = require('redis').createClient();
var mongoose = require('mongoose');
var request = require('request');
var cheerio = require('cheerio');

var conf = require('jobs/__template__');
var Queue = require('bin/queue');
console.log(Queue.get_feed_list_status);

// 数据提取函数
var refine = function(raw) {

};

// 初始化 redis
redis.on('error', function(err) {
	console.log('Redis Error: ' + err);
});


// 初始化 mongoose
//mongoose.connect('mongodb://localhost/' + db_name);
//var Rec = mongoose.model(table_name, table_schema);

// 处理一条 list
var process_list = function(url) {
	var options = {
		url: url,
		headers: headers,
		timeout: timeout
	};

	function callback(err, res, body) {
		if (!err && res.statusCode == 200) {
			var items = body.match(item_regex);
			_.each(items, function(item) {
				console.log(item);
				redis.sadd('items', item.split('_')[1]);
			});
		} else {
			// 如果状态不正常，则记录信息，并将url放回redis
			redis.sadd('lists', url);
		}
	}

	request(options, callback);

};

// 处理一条 item
var process_item = function(id) {
	var url = base_item_url.replace(/__ID__/, id);
	console.log(url);
};

// 发送状态邮件
var send_status = function(title, content) {

};



var run = function() {

	// 初始化程序
	if (redis.get('initialized') != 'yes') {
		var lists = _.sample(generate_list(), 2);
		_.each(lists, function(url) {
			redis.sadd('lists', url);
		});
		//redis.set('initialized', 'yes');
		//edis.set('done', '0');
		console.log('列表页面已载入，初始化完成');
	} else {
		console.log('无需初始化');
	}

	// 处理 list 页面
	var list_timer = setInterval(function() {

		process_list(redis.spop('lists'));

		if (redis.scard('lists') <= 0) {
			console.log('list 页面处理完成');
			clearInterval(list_timer);
		}
	}, interval);

	// 处理 item 页面
	var item_timer = setInterval(function() {

		process_item(redis.spop('items'));

		if (redis.scard('lists') <= 0 && redis.scard('items') <= 0) {
			console.log('item 页面处理完成');
			clearInterval(item_timer);
			mongoose.connection.close();
			redis.quit();
		}
	}, interval);
};