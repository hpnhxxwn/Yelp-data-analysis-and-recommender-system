var source = new EventSource('/stream');
var hash = {};
var width = 1200;
var height = 700;

source.onmessage = function (event) {

  document.body.innerHTML += event.data + '<br>';
};

