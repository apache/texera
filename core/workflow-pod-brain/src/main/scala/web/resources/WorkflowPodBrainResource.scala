package web.resources

import jakarta.ws.rs.core.MediaType
import jakarta.ws.rs.{Path, Produces}
import org.jooq.types.UInteger

object WorkflowPodBrainResource {
  // TODO: consider add the workflow json as the WorkflowPodCreationParams
  case class WorkflowPodCreationParams(wid: UInteger)

  case class WorkflowPodTerminationParams(wid: UInteger)
}

@Produces(Array(MediaType.APPLICATION_JSON))
@Path("/workflowpod")
class WorkflowPodBrainResource {
//  /**
//    * Create a new pod for the given workflow wid and workflow content
//    * @param user current user
//    * @param param the parameters
//    * @return the created pod
//    */
//  @POST
//  @Consumes(Array(MediaType.APPLICATION_JSON))
//  @Produces(Array(MediaType.APPLICATION_JSON))
//  @Path("/create")
//  def createPod(
//                 param: WorkflowPodCreationParams
//               ): WorkflowPod = ???
//
//
//  /**
//    * List all pods created by current user
//    * @return
//    */
//  @GET
//  @Path("")
//  def listPods(): List[WorkflowPod] = ???
//
//
//  /**
//    * Terminate the workflow's pod
//    * @param param
//    * @return
//    */
//  @POST
//  @Consumes(Array(MediaType.APPLICATION_JSON))
//  @Produces(Array(MediaType.APPLICATION_JSON))
//  @Path("/terminate")
//  def terminatePod(
//                    param: WorkflowPodTerminationParams
//                  ): Response = ???
}
