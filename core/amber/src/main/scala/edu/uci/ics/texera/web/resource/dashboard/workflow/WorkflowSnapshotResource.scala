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

    def getSnapshotbyId(sid: UInteger): WorkflowSnapshot = {
        snapshotDao.fetchOneBySid(sid)
    }

    /**
    * This function retrieves the latest snapshot of a workflow
    * @return sid
    */
    def getLatestSnapshot(): UInteger = {
        val snapshots = context
            .select(WORKFLOW_SNAPSHOT.SID)
            .from(WORKFLOW_SNAPSHOT)
            .fetchInto(classOf[UInteger])
            .toList
        snapshots.max
    }

    private def insertSnapshot(snapshotBlob: Array[Byte]): Unit = {
        val newSnapshot = new WorkflowSnapshot()
        newSnapshot.setSnapshot(snapshotBlob: _*)
        snapshotDao.insert(newSnapshot)
    }
}

@PermitAll
@Path("/snapshot")
@Produces(Array(MediaType.APPLICATION_JSON))
class WorkflowSnapshotResource {
    
    @GET 
    @Path("/{sid}")
    def retrieveWorkflowSnapshot(
        @PathParam("sid") sid: UInteger,
        @Auth sessionUser: SessionUser
    ): WorkflowSnapshot = {
        getSnapshotbyId(sid)
    }

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
            insertSnapshot(snapshotBlob)
        }
    }
}
