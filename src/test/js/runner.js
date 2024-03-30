var puppeteer = require('puppeteer');

if (process.argv.length !== 3) {
  console.log('Expected a target URL parameter.');
  process.exit(1);
}

(async ()  => {
    const browser = await puppeteer.launch({ headless: true }); // Launch headless Chrome
    const page = await browser.newPage(); // Create a new page

    // test html file
    var url = 'file://' + process.cwd() + '/' + process.argv[2];

    await page.goto(url);

    page.on('console', async (msg) => {
      const msgArgs = msg.args();
      for (let i = 0; i < msgArgs.length; ++i) {
        console.log(await msgArgs[i].jsonValue());
      }
    });

    var success = await page.evaluate(() => {
        return clara.test.run();
    })

    await browser.close();

    return success;
})().then(success =>
  {
  if (success){
      process.exit(0);
  } else {
      process.exit(1);
  }
})





