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

  page.evaluate(function() {
    clara.test.run();
  });

  setTimeout(function() {
    phantom.exit(0);
  }, 0);
});
