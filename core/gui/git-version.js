const { gitDescribeSync } = require("git-describe");
const { version } = require("./package.json");
const { resolve, relative } = require("path");
const { writeFileSync, existsSync, mkdirSync } = require("fs-extra");

const gitInfo = gitDescribeSync({
  dirtyMark: false,
  dirtySemver: false,
});

gitInfo.version = version;

if (!existsSync(__dirname + "/src/environments")) {
  mkdirSync(__dirname + "/src/environments");
}
const file = resolve(__dirname, "src", "environments", "version.ts");
writeFileSync(
  file,
  `// IMPORTANT: THIS FILE IS AUTO GENERATED! DO NOT MANUALLY EDIT OR CHECKIN!
/* tslint:disable */
export const Version = ${JSON.stringify(gitInfo, null, 4)};
/* tslint:enable */
`,
  { encoding: "utf-8" }
);

console.log(`Wrote version info ${gitInfo.raw} to ${relative(resolve(__dirname, ".."), file)}`);
