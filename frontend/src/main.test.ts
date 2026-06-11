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

// Minimal entry for the test-only build configuration. The unit-test
// builder uses the buildTarget's `main` to seed the bundle graph; pointing
// it at the real `main.ts` pulls AppModule (and every component declared
// there) into the spec compile, surfacing template type-check failures
// for components that no active spec actually references. This stub
// keeps the bundle graph minimal so only the modules each spec imports
// directly get compiled. Tests' Angular setup is provided by the unit-test
// builder itself (TestBed init).
export {};
