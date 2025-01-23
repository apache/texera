module.exports = {
  module: {
    rules: [
      {
        test: /\.css$/,
        use: ["style-loader", "css-loader"],
        include: [
          require("path").resolve(__dirname, "node_modules/monaco-editor"),
          require("path").resolve(__dirname, "node_modules/monaco-breakpoints")
        ],
      },
    ],
    // this is required for loading .wasm (and other) files.
    // For context, see https://stackoverflow.com/a/75252098 and https://github.com/angular/angular-cli/issues/24617
    parser: {
      javascript: {
        url: true,
      },
    },
  },
};
