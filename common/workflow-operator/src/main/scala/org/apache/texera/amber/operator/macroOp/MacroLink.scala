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

import com.fasterxml.jackson.annotation.JsonProperty
import org.apache.texera.amber.core.workflow.PortIdentity

// Mirrors LogicalLink's shape but lives in workflow-operator (which doesn't depend
// on workflow-compiling-service where LogicalLink lives). MacroExpander converts
// MacroLink → LogicalLink when inlining a macro body into the parent plan.
case class MacroLink(
    @JsonProperty("fromOpId") fromOpId: String,
    fromPortId: PortIdentity,
    @JsonProperty("toOpId") toOpId: String,
    toPortId: PortIdentity
)
