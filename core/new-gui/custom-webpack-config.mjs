import { fileURLToPath } from 'url';
import { dirname, resolve } from 'path';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

const config = {
    module: {
        rules: [{
            test: /\.css$/,
            use: ['style-loader', 'css-loader'],
            include: [
                resolve(__dirname, '../../../node_modules/monaco-editor')
            ]
        }, {
            test: /\.ttf$/,
            use: ['file-loader']
        }]
    },
    resolve: {
        extensions: ['.ts', '.js'],
        fallback: {
            path: resolve(__dirname, '../../../node_modules/path-browserify')
        }
    }
};

export default config;
