module.exports = {
  module: {
    rules: [
      {
        test: /\.css$/,
        use: ["style-loader", "css-loader"],
        include: [require("path").resolve(__dirname, "node_modules/monaco-editor")],
      },
    ],
  },
};
