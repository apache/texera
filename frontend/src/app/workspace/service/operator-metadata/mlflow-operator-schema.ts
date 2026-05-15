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

import { GroupInfo, OperatorSchema } from "../../types/operator-schema.interface";

/** Frontend-injected operator schema for MLFlow (mimics CSV File Scan with dataset selection). */
export const MLFLOW_OPERATOR_SCHEMA: OperatorSchema = {
  operatorType: "MLFlow",
  jsonSchema: {
    type: "object",
    properties: {
      fileName: {
        type: "string",
        title: "file name",
        description: "Select a file from your datasets (e.g. ML model file)",
      },
    },
    required: ["fileName"],
  },
  additionalMetadata: {
    userFriendlyName: "MLFlow",
    operatorDescription: "Load an ML model or data file from your datasets",
    operatorGroupName: "Machine Learning",
    inputPorts: [],
    outputPorts: [{}],
  },
  operatorVersion: "1.0",
};

export const MACHINE_LEARNING_GROUP_NAME = "Machine Learning";

/** Group entry for the operator menu. */
export const MACHINE_LEARNING_GROUP: GroupInfo = {
  groupName: MACHINE_LEARNING_GROUP_NAME,
};
