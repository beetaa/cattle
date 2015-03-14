var request = require('request');

function generate_list() {

}

var options = {
	url: 'http://wiki.ciwong.com/Question/SearchQuestions?periodId=1003&gradeId=1107&sectionId=1201&subjectId=9&pointCode=&unitid=&questionTypes=&rangeTypes=&pageIndex=1',
	json: true
};

request(options, function(err, res, objects) {
	console.log(objects);
	console.log(objects.PageIndex);
});