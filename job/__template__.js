var _ = require('underscore');
var cheerio = require('cheerio');

var generate_list_urls = function() {

	var list_urls = [];
	
	var resource = [
		[9, 293999], [10, 118983], [11, 93223], [24, 3031], [20, 4794], 
		[19, 1952], [18, 128], [32, 3289], [29, 130668], [1, 177584], 
		[2, 14246], [3, 15521], [7, 5369], [6, 9410], [4, 25759], [5, 24875]
	];
	var per_page = 20;

	_.each(resource, function(item) {
		var pages = _.range(1, Math.ceil(item[1] / per_page) + 1);
		_.each(pages, function(page) {
			list_urls.push('http://www.sosoti.com/index/list/' + item[0] + '/0/0/0/20/' + page);
		});
	});

	return list_urls;
}; 


var discover_item_urls = function(body) {
	var item_url_pattern = /http\:\/\/www\.sosoti\.com\/index\/SubjectDetail\/\d+/ig;
	try {
		var item_urls = body.match(item_url_pattern);
		if (item_urls.length > 0) {
			return item_urls;
		} else {
			return false;
		}
	} catch (e) {
		return false;
	}
};

var refine_item_data = function(body) {
	var item_data = {};

	try {
		//获取内容根部
		var $ = cheerio.load(body)('div.web_right_cent_detail');
		//解析id、点击率、题型
		var $info = $.children('div.web_right_cent_a').find('li');
		item_data.id = $info.eq(0).text();
		item_data.click = $info.eq(1).text();
		item_data.type = $info.eq(2).text();
		//解析关联的章节、知识点
		var $relate = $.children('div#ChapterArea').find('div');
		item_data.chapter = $relate.eq(0).text();
		item_data.point = $relate.eq(1).text();
		//解析题干
		item_data.tigan = $.children('div.web_right_cent_b_detail').html();
		//解析提示、详解、答案
		var $solve = $.children('div.web4_left_t_fo');
		item_data.tip = $solve.eq(0).html();
		item_data.analyse = $solve.eq(1).html();
		item_data.answer = $solve.eq(2).html();
		
	}
	catch(e) {
		return false;
	}

	return item_data;
};

var is_list_page = function(url) {
	var list_url_char = 'list';
	
	if (url.indexOf(list_url_char) >= 0) {
		return true;
	}

	return false;
};

module.exports = {

	debug: false,
	debug_list_page_count: 2,

	// 数据处理函数
	discover_item_urls: discover_item_urls,
	refine_item_data: refine_item_data,
	is_list_page: is_list_page,
	generate_list_urls: generate_list_urls,


	task_name: 'sosoti',
	// 数据库名称
	db_name: 'river',
	// 数据表名称
	item_table_name: 'Sosoti',
	// 数据表定义
	item_table_schema: {
		url: {type: String},
		raw: {type: String},
		id: {type: String},
		click: {type: String},
		type: {type: String},
		chapter: {type: String},
		point: {type: String},
		tigan: {type: String},
		tip: {type: String},
		analyse: {type: String},
		answer: {type: String}
	},

	// 子进程数量
	process_count: 5,
	// 每一次请求的间隔时间，缺省为 0.25 秒
	interval: 500,

	// 每次请求网页的选项配置
	request_options: {
		url: '',
		headers: {
			'Referer': 'http://sosoti.com/',
			'User-Agent': 'Mozilla/5.0 (Windows NT 5.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/32.0.1700.76 Safari/537.36'
		},
		timeout: 20000
	},

	// 邮件设置
	send_mail_interval: 1800000,
	smtp_options: {
		service: 'QQ',
		auth: {
			user: '2214431392@qq.com',
			pass: 'zhaomeng'
		}
	}
	
};