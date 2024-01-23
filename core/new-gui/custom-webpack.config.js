const path = require('path');

module.exports = {
  module: {
    rules: [
      {
        test: /(codicon.css$)/i,
        type: 'asset'
      },
      {
        test: /\.css$/,
        use: ['style-loader', 'css-loader'],
        include: [
          path.resolve(__dirname, 'node_modules/monaco-editor')
        ]
      }
    ]
  }
};
