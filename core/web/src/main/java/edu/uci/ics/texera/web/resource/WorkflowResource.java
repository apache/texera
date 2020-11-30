package edu.uci.ics.texera.web.resource;

import edu.uci.ics.texera.dataflow.jooq.generated.tables.daos.WorkflowDao;
import edu.uci.ics.texera.dataflow.jooq.generated.tables.daos.WorkflowOfUserDao;
import edu.uci.ics.texera.dataflow.jooq.generated.tables.pojos.Workflow;
import edu.uci.ics.texera.dataflow.jooq.generated.tables.pojos.WorkflowOfUser;
import edu.uci.ics.texera.dataflow.sqlServerInfo.SqlServer;
import io.dropwizard.jersey.sessions.Session;
import org.glassfish.jersey.media.multipart.FormDataParam;
import org.jooq.types.UInteger;

import javax.servlet.http.HttpSession;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import java.util.ArrayList;
import java.util.List;

import static edu.uci.ics.texera.dataflow.jooq.generated.Tables.WORKFLOW;
import static edu.uci.ics.texera.dataflow.jooq.generated.Tables.WORKFLOW_OF_USER;

/**
 * This file handles various request related to saved-workflows.
 * It sends mysql queries to the MysqlDB regarding the UserWorkflow Table
 * The details of UserWorkflowTable can be found in /core/scripts/sql/texera_ddl.sql
 */

@Path("/workflow")
@Produces(MediaType.APPLICATION_JSON)
public class WorkflowResource {

    private final WorkflowDao workflowDao = new WorkflowDao(SqlServer.createDSLContext().configuration());
    private final WorkflowOfUserDao workflowOfUserDao = new WorkflowOfUserDao(SqlServer.createDSLContext().configuration());


    @GET
    @Path("/get")
    public List<Workflow> getUserWorkflow(@Session HttpSession session) {

        UserResource.User user = UserResource.getUser(session);
        if (user == null) return new ArrayList<Workflow>() {
        };

        // uncomment below to link user with workflow
        // UInteger userID =
        return getWorkflowByUser(user.getUserID());
    }

    /**
     * This method handles the frontend's request to get a specific workflow to be displayed
     * at current design, it only takes the workflowID and searches within the database for the matching workflow
     * for future design, it should also take userID as an parameter.
     *
     * @param workflowID workflow id, which serves as the primary key in the UserWorkflow database
     * @param session
     * @return a json string representing an savedWorkflow
     */
    @GET
    @Path("/get/{workflowID}")
    public Workflow getWorkflow(@PathParam("workflowID") UInteger workflowID,
                                @Session HttpSession session) {

        return workflowDao.fetchOneByWid(workflowID);
    }

    @POST
    @Path("/save-workflow")
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    public Workflow saveWorkflow(
            @Session HttpSession session,
            @FormDataParam("wfId") UInteger wfId,
            @FormDataParam("name") String name,
            @FormDataParam("content") String content
    ) {
        UserResource.User user = UserResource.getUser(session);
        if (user == null) {
            return null;
        }
        if (wfId != null) {
            return updateWorkflow(wfId, name, content);
        }
        Workflow workflow = insertWorkflowToDataBase(name, content);
        workflowOfUserDao.insert(new WorkflowOfUser(user.getUserID(), workflow.getWid()));

        return workflow;

    }

    /**
     * select * from table workflow where workflowID is @param "workflowID"
     *
     * @param userId
     * @return
     */
    private List<Workflow> getWorkflowByUser(UInteger userId) {
        return SqlServer.createDSLContext()
                .select(WORKFLOW.fields())
                .from(WORKFLOW).join(WORKFLOW_OF_USER).on(WORKFLOW_OF_USER.WID.eq(WORKFLOW.WID))
                .where(WORKFLOW_OF_USER.UID.eq(userId))
                .fetchInto(Workflow.class);
    }


    /**
     * update table workflow set content = @param "content" where wid = @param "workflowId"
     *
     * @param workflowId
     * @param name
     * @param content
     * @return
     */
    private Workflow updateWorkflow(UInteger workflowId, String name, String content) {
        SqlServer.createDSLContext().update(WORKFLOW)
                .set(WORKFLOW.NAME, name)
                .set(WORKFLOW.CONTENT, content)
                .where(WORKFLOW.WID.eq(workflowId)).execute();
        return workflowDao.fetchOneByWid(workflowId);

    }

    /**
     * This private method will be used to insert a non existing workflow into the database
     * There is no request handler that utilize this method yet.
     * <p>
     * //     * @param userID
     * //     * @param workflowID
     *
     * @param workflowName
     * @param content
     * @return
     */
    private Workflow insertWorkflowToDataBase(String workflowName, String content) {
        Workflow workflow = new Workflow();
        workflow.setName(workflowName);
        workflow.setContent(content);
        workflowDao.insert(workflow);
        return workflow;
    }
}
