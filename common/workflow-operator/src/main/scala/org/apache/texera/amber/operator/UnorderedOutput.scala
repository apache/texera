/*
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

package org.apache.texera.amber.operator

/**
  * Marker for operators whose output row ORDER is not part of their contract:
  * they emit a set/bag whose ordering is implementation-defined (e.g. hash-set
  * iteration, hash-partitioned group emit, join bucket order). Consumers that
  * must not rely on a stable row order should read this instead of hard-coding a
  * per-operator list.
  *
  * The behavioral-parity verifier uses it to compare such operators
  * order-insensitively (both paths are lexicographically sorted first), because
  * the platform executor and the generated standalone code may legitimately emit
  * the same rows in different orders. Absence of this trait means the output row
  * order IS significant (the default, strict comparison).
  */
trait UnorderedOutput
