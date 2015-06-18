
var page = require('webpage').create();
var url = phantom.args[0];

page.onConsoleMessage = function (message) {
    console.log(message);
};

function exit(code) {
    setTimeout(function(){ phantom.exit(code); }, 0);
    phantom.onError = function(){};
}

console.log("Loading URL: " + url);

page.open(url, function (status) {
    if (status != "success") {
        console.log('Failed to open ' + url);
        phantom.exit(1);
    }

    console.log("Running test.");

    var failures = page.evaluate(function() {
        test_runner.runner();
        return window["test-failures"];
    });

    if (failures == 0) {
        console.log("Tests succeeded.")
    }
    else {
        console.log("*** Tests failed! ***");
    }
    
    phantom.exit(failures?100:0);
});
