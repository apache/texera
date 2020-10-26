package edu.uci.ics.texera.web.resource;

import com.fasterxml.jackson.databind.node.ObjectNode;
import edu.uci.ics.texera.dataflow.jooq.generated.tables.records.WorkflowRecord;
import edu.uci.ics.texera.dataflow.sqlServerInfo.UserSqlServer;
import edu.uci.ics.texera.web.TexeraWebException;
import edu.uci.ics.texera.web.response.GenericWebResponse;
import io.dropwizard.jersey.sessions.Session;
import org.glassfish.jersey.media.multipart.FormDataParam;
import org.jooq.Record1;
import org.jooq.Record3;
import org.jooq.types.UInteger;

import javax.servlet.http.HttpSession;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;

import java.util.ArrayList;
import java.util.List;

import static edu.uci.ics.texera.dataflow.jooq.generated.Tables.*;

/**
 * This file handles various request related to saved-workflows.
 * It sends mysql queries to the MysqlDB regarding the UserWorkflow Table
 * The details of UserWorkflowTable can be found in /core/scripts/sql/texera_ddl.sql
 */

// uncomment and use below to give workflows the concept of ownership
// @Path("/user/workflow")
@Path("/workflow")
@Produces(MediaType.APPLICATION_JSON)
public class WorkflowResource {

    public static class Workflow {
        private final UInteger wfId;
        private final UInteger userId; // the ID in MySQL database is unsigned int
        private final String content;


        public Workflow(UInteger wfId, UInteger userId, String content) {
            this.wfId = wfId;
            this.userId = userId;
            this.content = content;
        }


        public String getContent() {
            return content;
        }

        public UInteger getUserId() {
            return userId;
        }

        public UInteger getWfId() {
            return wfId;
        }
    }

    /**
     * Corresponds to `src/app/common/type/workflow.ts`
     */
    public static class WorkflowWebResponse {
        private int code;
        private String message;
        private Workflow workflow;

        public static WorkflowWebResponse generateErrorResponse(String message) {
            return new WorkflowWebResponse(1, message, null);
        }

        public WorkflowWebResponse() {
            // Default constructor is required for Jackson JSON serialization
        }

        public static WorkflowWebResponse generateSuccessResponse(Workflow workflow) {
            return new WorkflowWebResponse(0, "success", workflow);
        }

        private WorkflowWebResponse(int code, String message, Workflow workflow) {
            this.code = code;
            this.message = message;
            this.workflow = workflow;
        }

        public Workflow getWorkflow() {
            return workflow;
        }

        public void setWorkflow(Workflow workflow) {
            this.workflow = workflow;
        }

        public int getCode() {
            return code;
        }

        public void setCode(int code) {
            this.code = code;
        }

        public String getMessage() {
            return message;
        }

        public void setMessage(String message) {
            this.message = message;
        }
    }


    @GET
    @Path("/get")
    public List<Workflow> getUserWorkflow(@Session HttpSession session) {
        UInteger userId = UserResource.getUser(session).getUserID();
        if (userId == null) {
            return new ArrayList<Workflow>() {
            };
//            return  new [Workflow(UInteger.valueOf(1), UInteger.valueOf(1), "all user")];
        }
        // uncomment below to link user with workflow
        // UInteger userID =
        return getWorkflowByUser(userId);
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

        if (workflowID == null) {
            return new Workflow(UInteger.valueOf(1), UInteger.valueOf(1), "all user");
        }
        // uncomment below to link user with workflow
        // UInteger userID =
        Record3<UInteger, String, String> result = getWorkflowFromDatabase(workflowID);

        if (result == null) {
            throw new TexeraWebException("Workflow with id: " + workflowID + " does not exit.");
        }
        return new Workflow(workflowID, UserResource.getUser(session).getUserID(), result.get(WORKFLOW.CONTENT));
    }

    /**
     * this method handles the frontend's request to save a specific workflow
     * at current design, it takes a workflowID and a JSON string representing the new workflow
     * it updates the corresponding mysql record; throws an error if the workflow does not exist
     * for future design, it should also take userID as an parameter.
     *
     * @param session
     * @param workflowID
     * @param workflowBody
     * @return
     */
    @POST
    @Path("/update-workflow")
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    public GenericWebResponse setUserWorkflow(
            @Session HttpSession session,
            @FormDataParam("workflowID") UInteger workflowID,
            @FormDataParam("workflowBody") String workflowBody
    ) {
        int count = checkWorkflowExist(workflowID);
        if (count != 1) {
            return new GenericWebResponse(1, "workflow " + workflowID + " does not exist in the database");
        }
        int result = updateWorkflowInDataBase(workflowID, workflowBody);
        throwErrorWhenNotOne("Error occurred while updating workflow to database", result);
        return GenericWebResponse.generateSuccessResponse();
    }

    @POST
    @Path("/save-workflow")
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    public Workflow saveWorkflow(
            @Session HttpSession session,
            @FormDataParam("wfId") UInteger wfId,
            @FormDataParam("content") String content
    ) {
        UInteger userId = UserResource.getUser(session).getUserID();
        if (wfId != null) {
            updateWorkflowInDataBase(wfId, content);
            return new Workflow(wfId, userId, content);
        }
        String name = "name";
        Record1<UInteger> newWfId = insertWorkflowToDataBase(name, content);
        WorkflowRecord workflowRecord = new WorkflowRecord(name, newWfId.value1(), content);
        //        throwErrorWhenNotOne("Error occurred while updating workflow to database", result);
        int result = insertWorkflowOfUser(newWfId.value1(), userId);
        throwErrorWhenNotOne("Error occurred while updating workflow to database", result);
        return new Workflow(workflowRecord.getWfId(), userId, workflowRecord.getContent());

    }

    /**
     * select * from table userworkflow where workflowID is @param "workflowID"
     *
     * @param workflowID
     * @return
     */
    private Record3<UInteger, String, String> getWorkflowFromDatabase(UInteger workflowID) {
        return UserSqlServer.createDSLContext()
                .select(WORKFLOW.WF_ID, WORKFLOW.NAME, WORKFLOW.CONTENT)
                .from(WORKFLOW)
                .where(WORKFLOW.WF_ID.eq(workflowID))
                .fetchOne();
    }

    /**
     * select * from table userworkflow where workflowID is @param "workflowID"
     *
     * @param userId
     * @return
     */
    private List<Workflow> getWorkflowByUser(UInteger userId) {
        return UserSqlServer.createDSLContext()
                .select(WORKFLOW.WF_ID, WORKFLOW.NAME, WORKFLOW.CONTENT)
                .from(WORKFLOW).join(WORKFLOW_OF_USER).on(WORKFLOW_OF_USER.WF_ID.eq(WORKFLOW.WF_ID))
                .where(WORKFLOW_OF_USER.UID.eq(userId))
                .fetchInto(Workflow.class);
    }


    /**
     * update table userworkflow set workflowBody = @param "workflowBody" where workflowID = @param "workflowID"
     *
     * @param workflowID
     * @param content
     * @return
     */
    private int updateWorkflowInDataBase(UInteger workflowID, String content) {
        return UserSqlServer.createDSLContext().update(WORKFLOW)
                .set(WORKFLOW.CONTENT, content)
                .where(WORKFLOW.WF_ID.eq(workflowID))
                .execute();
    }

    /**
     * select count(*) from userworkflow where workflowID = @param "workflowID"
     *
     * @param workflowID
     * @return
     */
    private int checkWorkflowExist(UInteger workflowID) {
        return UserSqlServer.createDSLContext()
                .selectCount()
                .from(WORKFLOW)
                .where(WORKFLOW.WF_ID.eq(workflowID))
                .fetchOne(0, int.class);
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
    private Record1<UInteger> insertWorkflowToDataBase(String workflowName, String content) {
        return UserSqlServer.createDSLContext().insertInto(WORKFLOW)
                // uncomment below to give workflows the concept of ownership
                // .set(USERWORKFLOW.USERID,userID)
                // .set(WORKFLOW.WF_ID, workflowID)
                .set(WORKFLOW.NAME, workflowName)
                .set(WORKFLOW.CONTENT, content)
                .returningResult(WORKFLOW.WF_ID)
                .fetchOne();
    }

    private int insertWorkflowOfUser(UInteger workflowId, UInteger userId) {
        return UserSqlServer.createDSLContext().insertInto(WORKFLOW_OF_USER)
                .set(WORKFLOW_OF_USER.WF_ID, workflowId)
                .set(WORKFLOW_OF_USER.UID, userId)
                .execute();
    }

    /**
     * Corresponds to interface SavedWorkflow in `src/app/workspace/service/save-workflow/save-workflow.service.ts`
     */
    public static class UserWorkflow {
        public UInteger workflowID;
        public String workflowName;
        public ObjectNode workflowBody;

        public UserWorkflow(UInteger id, String name, ObjectNode body) {
            this.workflowID = id;
            this.workflowName = name;
            this.workflowBody = body;
        }
    }

    /**
     * Most the sql operation should only be executed once. eg. insertion, deletion.
     * this method will raise TexeraWebException when the input number is not one
     *
     * @param errorMessage
     * @param count
     * @throws TexeraWebException
     */
    private void throwErrorWhenNotOne(String errorMessage, int count) throws TexeraWebException {
        if (count != 1) {
            throw new TexeraWebException(errorMessage);
        }
    }
}
