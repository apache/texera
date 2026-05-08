/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

// Worker entry — referenced via `new Worker(new URL('./editor.worker', import.meta.url))`
// from code-editor.component.ts. The relative-path spec is what webpack 5 recognises
// as a worker entry point (so it bundles the codingame dep tree into a chunk) and
// what esbuild can resolve against the filesystem during the @angular/build:unit-test
// spec pre-bundle. Inlining a `new URL("@codingame/...", import.meta.url)` directly in
// the component would satisfy webpack but trip esbuild, which treats the spec as a
// literal relative URL.
import "@codingame/monaco-vscode-editor-api/esm/vs/editor/editor.worker.js";
