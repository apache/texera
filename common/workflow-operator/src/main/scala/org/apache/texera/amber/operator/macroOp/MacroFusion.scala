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

package org.apache.texera.amber.operator.macroOp

import com.fasterxml.jackson.annotation.JsonIgnoreProperties

// AI-fusion payload (Section 9.2). When `verified = true`, MacroExpander substitutes
// the MacroOpDesc with a single PythonUDFOpDescV2 built from `code` instead of inlining
// the macro body. `sampleSize` records how many rows the sample-run diff matched on;
// `verifiedAt` is the epoch millis when verification passed.
//
// `ignoreUnknown = true`: the frontend attaches UI-only fields (e.g.
// `estimatedSpeedup`, a human-readable "1.6×" used to render the on-canvas
// ⚡ FUSED badge) onto this payload before persisting. The backend doesn't
// model those fields here; without this annotation Jackson rejects the
// whole WorkflowExecuteRequest at execute time.
@JsonIgnoreProperties(ignoreUnknown = true)
case class MacroFusion(
    code: String,
    verified: Boolean = false,
    sampleSize: Int = 0,
    verifiedAt: Long = 0L
)
