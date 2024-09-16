package web.resources

import io.kubernetes.client.openapi.models.V1Pod
import jakarta.ws.rs.core.{MediaType, Response}
import jakarta.ws.rs.{Consumes, GET, POST, Path, PathParam, Produces, QueryParam}
import org.jooq.DSLContext
import org.jooq.types.UInteger
import service.KubernetesClientService
import web.Utils.withTransaction
import web.model.SqlServer
import web.model.jooq.generated.tables.daos.PodDao
import web.resources.WorkflowPodBrainResource.{WorkflowPodCreationParams, WorkflowPodTerminationParams, WorkflowPodRunParams, context}
import web.model.jooq.generated.tables.pojos.Pod

import java.sql.Timestamp

object WorkflowPodBrainResource {

  private lazy val context: DSLContext = SqlServer.createDSLContext()
  case class WorkflowPodCreationParams(uid: UInteger, wid: UInteger)

  case class WorkflowPodTerminationParams(uid: UInteger, wid: UInteger)

  case class WorkflowPodRunParams(wid: UInteger, workflow: String)
}

@Produces(Array(MediaType.APPLICATION_JSON))
@Path("/workflowpod")
class WorkflowPodBrainResource {
  /**
   * Create a new pod for the given workflow wid and workflow content
   *
   * @param param the parameters
   * @return the created pod
   */
  @POST
  @Consumes(Array(MediaType.APPLICATION_JSON))
  @Produces(Array(MediaType.APPLICATION_JSON))
  @Path("/create")
  def createPod(
                 param: WorkflowPodCreationParams
               ): Response = {
    var newPod: V1Pod = null
    try {
      newPod = new KubernetesClientService().createUserPod(param.uid.intValue(), param.wid.intValue())
    } catch {
      case e: Exception => return Response.status(Response.Status.BAD_REQUEST).entity(e.getMessage).build()
    }

    // Set uid, wid, name, pod_uid, creation_time manually
    // terminate_time is set on pod termination
    val newSQLPod: Pod = new Pod()
    newSQLPod.setUid(param.uid)
    newSQLPod.setWid(param.wid)
    newSQLPod.setName(newPod.getMetadata.getName)
    newSQLPod.setPodUid(newPod.getMetadata.getUid)
    newSQLPod.setCreationTime(Timestamp.from(newPod.getMetadata.getCreationTimestamp.toInstant))

    try {
      // Insert the new pod into the database within a transaction
      withTransaction(context) { ctx =>
        val podDao = new PodDao(ctx.configuration())
        podDao.insert(newSQLPod)
      }
    } catch {
      case e: Exception => return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(e.getMessage).build()
    }

    // If everything succeeds, return the new SQL pod as the response entity
    Response.ok(newSQLPod).build()
  }


  /**
    * List all pods created by current user
    *
    * @return
    */
  @GET
  @Path("")
  @Produces(Array(MediaType.APPLICATION_JSON))
  def listPods(@QueryParam("uid") uid: UInteger): java.util.List[Pod] = {
    withTransaction(context) { ctx =>
      val podDao = new PodDao(ctx.configuration())
      var pods: java.util.List[Pod] = null
      if (uid == null) {
         pods = podDao.findAll()
      } else {
        pods = podDao.fetchByUid(uid)
      }
      pods.removeIf((pod: Pod) => pod.getTerminateTime != null)
      pods
    }
  }


  /**
    * Terminate the workflow's pod
    * @param param the parameters
    *
    * @return request response
    */
  @POST
  @Consumes(Array(MediaType.APPLICATION_JSON))
  @Produces(Array(MediaType.APPLICATION_JSON))
  @Path("/terminate")
  def terminatePod(
                    param: WorkflowPodTerminationParams
                  ): Response = {
    new KubernetesClientService().deletePod(param.uid.intValue(), param.wid.intValue())
    withTransaction(context) { ctx =>
      val podDao = new PodDao(ctx.configuration())
      val pods = podDao.fetchByUid(param.uid)
      pods.forEach(pod =>
        if (pod.getWid == param.wid && pod.getTerminateTime == null)
          pod.setTerminateTime(new Timestamp(System.currentTimeMillis()))
      )
      podDao.update(pods)
      Response.ok(s"Successfully terminated user pod of uid ${param.uid} and wid ${param.wid}").build()
    }
  }
}
