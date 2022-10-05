package edu.uci.ics.texera.web.resource.dashboard.workflow

import edu.uci.ics.texera.web.SqlServer
import org.glassfish.jersey.media.multipart.FormDataParam
import javax.annotation.security.PermitAll
import io.dropwizard.auth.Auth
import org.jooq.types.UInteger
import edu.uci.ics.texera.web.auth.SessionUser
import javax.ws.rs.core.MediaType
import javax.ws.rs._
import org.jooq.Condition
import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`
import edu.uci.ics.texera.web.model.jooq.generated.Tables.WORKFLOW_SNAPSHOT
import edu.uci.ics.texera.web.resource.dashboard.workflow.WorkflowAccessResource.WorkflowAccess
import edu.uci.ics.texera.web.model.jooq.generated.tables.daos.WorkflowSnapshotDao
import edu.uci.ics.texera.web.model.jooq.generated.tables.pojos.WorkflowSnapshot
import edu.uci.ics.texera.web.resource.dashboard.workflow.WorkflowSnapshotResource._

object WorkflowSnapshotResource {
  final private lazy val context = SqlServer.createDSLContext()
  private val snapshotDao = new WorkflowSnapshotDao(context.configuration)

  /**
    *  This function get snapshot by the sid
    */
  def getSnapshotbyId(sid: UInteger): WorkflowSnapshot = {
    snapshotDao.fetchOneBySid(sid)
  }

  /**
    * This function retrieves the latest snapshot of a certain workflow
    * @return sid
    */
  def getLatestSnapshot(wid: UInteger): (Boolean, UInteger) = {
    val snapshots = context
      .select(WORKFLOW_SNAPSHOT.SID)
      .from(WORKFLOW_SNAPSHOT)
      .where(WORKFLOW_SNAPSHOT.WID.eq(wid))
      .fetchInto(classOf[UInteger])
      .toList
    if (snapshots.size < 2) {
      return (true, snapshots.max)
    }
    (false, snapshots.max)
  }

  /**
    * This function insert new snapshot into sql
    * @param snapshotBlob
    */
  private def insertSnapshot(wid: UInteger, snapshotBlob: Array[Byte]): Unit = {
    val newSnapshot = new WorkflowSnapshot()
    newSnapshot.setSnapshot(snapshotBlob: _*)
    newSnapshot.setWid(wid)
    snapshotDao.insert(newSnapshot)
  }

  /**
    * This function delete the snapshot entry of sid in sql
    * @param sid
    */
  def deleteSnapshot(sid: UInteger): Unit = {
    context
      .delete(WORKFLOW_SNAPSHOT)
      .where(WORKFLOW_SNAPSHOT.SID.eq(sid))
      .execute();
  }
}

@PermitAll
@Path("/snapshot")
@Produces(Array(MediaType.APPLICATION_JSON))
class WorkflowSnapshotResource {

  /**
    * This function retrieve the snapshot from sql by sid
    * @param sid
    * @param sessionUser
    * @return
    */
  @GET
  @Path("/{sid}")
  def retrieveWorkflowSnapshot(
      @PathParam("sid") sid: UInteger,
      @Auth sessionUser: SessionUser
  ): WorkflowSnapshot = {
    getSnapshotbyId(sid)
  }

  /**
    * This function insert new snapshot into sql
    * @param wid
    * @param snapshotBlob
    * @param sessionUser
    */
  @PUT
  @Path("/upload")
  @Consumes(Array(MediaType.MULTIPART_FORM_DATA))
  def uploadWorkflowSnapshot(
      @FormDataParam("wid") wid: UInteger,
      @FormDataParam("SnapshotBlob") snapshotBlob: Array[Byte],
      @Auth sessionUser: SessionUser
  ): Unit = {
    val user = sessionUser.getUser
    if (snapshotBlob == null) {
      throw new BadRequestException("Snapshot Blob cannot be null.")
    } else if (wid == null) {
      throw new BadRequestException("Cannot upload workflow snapshot without a provided id.")
    } else if (
      WorkflowAccessResource.hasNoWorkflowAccess(wid, user.getUid) ||
      WorkflowAccessResource.hasNoWorkflowAccessRecord(wid, user.getUid)
    ) {
      throw new ForbiddenException("No sufficient access privilege.")
    } else {
      insertSnapshot(wid, snapshotBlob)
    }
  }
}
