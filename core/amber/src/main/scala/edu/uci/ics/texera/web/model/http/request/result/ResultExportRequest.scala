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

package edu.uci.ics.texera.web.model.http.request.result

case class ResultExportRequest(
    exportType: String, // e.g. "csv", "google_sheet", "arrow", "data"
    workflowId: Int,
    workflowName: String,
    operatorIds: List[String], // changed from single operatorId: String -> List of strings
    datasetIds: List[Int],
    rowIndex: Int, // used by "data" export
    columnIndex: Int, // used by "data" export
    filename: String, // optional filename override
    destination: String // "dataset" or "local"
)
