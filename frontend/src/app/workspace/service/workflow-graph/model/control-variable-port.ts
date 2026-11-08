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

import { OperatorLink } from "../../../types/workflow-common.interface";

/**
 * The control-variable port that every operator has besides its data ports. Whatever arrives on it
 * becomes control variables of the operator, which a property refers to as "$name". It is drawn on
 * the canvas only, purple, at the bottom of the operator; it is not one of the operator's input
 * ports, so it never counts as a data input. The backend knows it by the reserved port id -1.
 */
export const CONTROL_VARIABLE_PORT_ID = "control-variables";
export const CONTROL_VARIABLE_PORT_GROUP = "control";
export const CONTROL_VARIABLE_COLOR = "#8E44AD";
export const CONTROL_VARIABLE_PORT_IDENTITY = { id: -1, internal: false };

export function isControlVariableLink(link: OperatorLink): boolean {
  return link.target.portID === CONTROL_VARIABLE_PORT_ID;
}
