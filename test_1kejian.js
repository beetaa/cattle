var request = require('request');
var request = request.defaults({jar: true});
var fs = require('fs');

var first_url = 'http://www.1kejian.com/shiti/softdown.asp?softid=150703';

var options = {
	method: 'POST',
    url: 'http://www.1kejian.com/shiti/download.asp',
    headers: {
    	'User-Agent': 'Mozilla/5.0 (Windows NT 5.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/32.0.1700.102 Safari/537.36',
        'Host': 'www.1kejian.com',
        'Referer': 'http://www.1kejian.com/shiti/softdown.asp?softid=150703'
    },
    form: {
    	softid: '150703', id: '150876', downid: '39', rnd: '7349'
    }
};

request(first_url, function() {

	request(options).pipe(fs.createWriteStream('my.rar'));
});


