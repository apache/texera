// Karma configuration file
module.exports = function (config) {
  config.set({
    basePath: "",
    frameworks: ["jasmine", "@angular-devkit/build-angular"],
    plugins: [
      require("karma-jasmine"),
      require("karma-chrome-launcher"),
      require("@angular-devkit/build-angular/plugins/karma")
    ],
    client: {
      clearContext: config.singleRun, // Leave Jasmine Spec Runner output visible in the browser
      jasmine: {
        random: false, // Disable random order for consistent test results
      },
    },
    customLaunchers: {
      ChromeHeadlessCustom: {
        base: "ChromeHeadless",
        flags: [
          "--no-sandbox",
          "--headless=new",
          "--remote-debugging-port=9222", // Enable remote debugging for better error output
          "--disable-gpu",
          "--disable-translate",
          "--disable-extensions",
          "--disable-dev-shm-usage", // Avoid /dev/shm issues in CI environments
          "--disable-extensions",
          "--disable-background-networking",
          "--disable-background-timer-throttling",
          "--disable-backgrounding-occluded-windows",
          "--disable-breakpad",
          "--disable-sync",
        ],
      },
    },
    reporters: ["dots"], // Use dots reporter
    port: 9876, // Karma's web server port
    colors: true, // Enable colors in the output (reporters and logs)
    logLevel: config.LOG_INFO, // Set log level
    autoWatch: false, // Disable auto-watch to prevent re-runs in CI
    concurrency: 1, // Launch only one browser at a time
    browsers: ["ChromeHeadlessCustom"], // Run tests in headless Chrome
    singleRun: true, // Ensure Karma exits after running tests once (useful for CI)
    restartOnFileChange: false, // Disable file change restarts in CI
    captureTimeout: 30000, // 30-second timeout for capturing the browser
    browserDisconnectTimeout: 60000, // 60-second disconnect timeout
    browserDisconnectTolerance: 1, // Allow up to 1 disconnect before failing
    browserNoActivityTimeout: 60000, // 60-second no-activity timeout
  });
};
