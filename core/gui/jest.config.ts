import type { Config } from "jest";

const esModules = [
  "lodash-es",
  "@ngrx",
  "@angular",
  "ng-zorro-antd",
  "@ant-design",
  "uuid",
  "@ngx-formly",
  "monaco-editor",
  "vscode",
  "monaco-editor-wrapper",
  "y-monaco",
  "@codingame",
  "monaco-languageclient",
  "@codingame/monaco-editor-wrapper",
].join("|");

const config: Config =  {
  preset: "jest-preset-angular",
  setupFilesAfterEnv: [`${__dirname}/setup.ts`],
  moduleNameMapper: {
    "\\.(jpg|jpeg|png)$": `${__dirname}/mock-module.js`,
  },
  transformIgnorePatterns: [`<rootDir>/node_modules/(?!${esModules})`],
  transform: {
    "^.+\\.(ts|tsx)?$": [
      "ts-jest",
      {
        allowSyntheticDefaultImports: true,
      },
    ],
    "^.+\\.(js|mjs)$": "babel-jest",
  },
  testEnvironment: "jsdom", // Set the environment to jsdom
};


export default config;
