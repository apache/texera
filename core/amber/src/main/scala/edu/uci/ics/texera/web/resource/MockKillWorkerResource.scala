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

//package edu.uci.ics.texera.web.resource
//
//import .KillAndRecover
//import javax.ws.rs.core.MediaType
//import javax.ws.rs.{POST, Path, Produces}
//
//@Path("/kill")
//@Produces(Array(MediaType.APPLICATION_JSON))
//class MockKillWorkerResource() {
//
//  @POST
//  @Path("/worker") def mockKillWorker: Unit = {
//    WorkflowWebsocketResource.sessionJobs.foreach(p => {
//      val controller = p._2._2
//      Thread.sleep(1500)
//      controller ! KillAndRecover
//    })
//  }
//
//}
