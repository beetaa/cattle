var _ = require('underscore');
var redis = require('redis').createClient();

exports.init = function(task_name) {
	
	// 此处应检验 task_name 的合法性
	var task_name = task_name || 'test';

	var data_paths = {
		feed_list_ok: task_name + ':feed_list_ok',
		success_count: task_name + ':success_count',

		waiting: task_name + ':waiting',
		
		running_count: task_name + ':running_count',
		
		error: task_name + ':error',
		not200: task_name + ':not200',
		refine_data_error: task_name + ':refine_data_error',
		save_data_error: task_name + ':save_data_error',
	};

	// 初始化闭包类
	var Queue = {};

	Queue.waiting_path = data_paths.waiting;

	Queue.push = function(path_name, url) {
		redis.sadd(data_paths[path_name], url);
	};


	Queue.update_success_count = function() {
		redis.incr(data_paths.success_count);
	};

	// 写入列表页面
	Queue.feed_list_urls = function(list_urls) {

		try {
			_.each(list_urls, function(url) {
				redis.sadd(data_paths.waiting, url);
			});

			redis.set(data_paths.feed_list_ok, 'ok');
		}
		catch(e) {
			console.log('QUEUE - 写入列表页面错误。');
		}

	};

	// 擦除配置数据
	Queue.clear_data = function() {

		try {
			_.each(_.values(data_paths), function(data_path) {
				redis.del(data_path);
			});
		}
		catch(e) {
			console.log('QUEUE - 擦除数据发生错误。');
		}

	};

	return Queue;
};