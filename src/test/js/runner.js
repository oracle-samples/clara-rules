var page = require('webpage').create();
var system = require('system');

if (system.args.length !== 2) {
  console.log('Expected a target URL parameter.');
  phantom.exit(1);
}

page.onConsoleMessage = function (message) {
  console.log(message);
};

var url = system.args[1];

page.open(url, function (status) {
    if (status !== "success") {
	console.log('Failed to open ' + url);
	setTimeout(function() {
	    phantom.exit(1);
	}, 0);
    }

    // Note that we have to return a primitive value from this function
    // rather than setting a closure variable.  The executed function is sandboxed
    // by PhantomJS and can't set variables outside its scope.
    var success = page.evaluate(function() {
	return clara.test.run();
    });

    setTimeout(function() {
	if (success){
	    phantom.exit(0);
	} else {
	    phantom.exit(1);
	}
    }, 0);
});
