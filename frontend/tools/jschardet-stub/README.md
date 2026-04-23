<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0
-->

# jschardet-stub

Apache-2.0 no-op replacement for the `jschardet` npm package.

## Why this exists

The upstream [`jschardet`](https://www.npmjs.com/package/jschardet)
package is LGPL-2.1, which is
[ASF Category X](https://www.apache.org/legal/resolved.html#category-x)
and cannot ship in an Apache binary distribution.

`jschardet` is declared as a direct dependency of
`@codingame/monaco-vscode-api@8.0.4`. Inside that package it is
referenced only from
`vscode/src/vs/workbench/services/textfile/common/encoding.js`, inside
`guessEncodingByBuffer()`, which is loaded lazily via a dynamic
`await import('jschardet')`. Texera does not open arbitrary binary
files through Monaco — the editor is wired up as a Yjs-backed code
editor — so the encoding-guessing path is never exercised.

## How it is wired in

`frontend/package.json` has a `resolutions` entry that redirects the
`jschardet` module name to this directory:

```json
"resolutions": {
  "jschardet": "portal:./tools/jschardet-stub"
}
```

Yarn then installs this stub in place of the real package. The stub
exports the same named functions (`detect`, `detectAll`, `enableDebug`)
and the same default shape, so the dynamic import and any defensive
caller keep working; `detect()` simply returns `null`, which the VS
Code encoding helper already treats as "no guess available".

## Scope

Do not use this package for anything else. It intentionally reports
that it cannot detect any charset.
