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
