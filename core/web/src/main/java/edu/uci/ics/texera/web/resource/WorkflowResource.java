package edu.uci.ics.texera.web.resource;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import edu.uci.ics.texera.dataflow.sqlServerInfo.UserSqlServer;
import edu.uci.ics.texera.web.TexeraWebException;
import edu.uci.ics.texera.web.response.GenericWebResponse;
import io.dropwizard.jersey.sessions.Session;
import org.glassfish.jersey.media.multipart.FormDataParam;
import org.jooq.Record3;
import org.jooq.types.UInteger;

import javax.servlet.http.HttpSession;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import java.io.IOException;

import static edu.uci.ics.texera.dataflow.jooq.generated.Tables.WORKFLOW;

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
    public UserWorkflow getUserWorkflow(@PathParam("workflowID") UInteger workflowID, @Session HttpSession session) {
        // uncomment below to link user with workflow
        // UInteger userID = UserResource.getUser(session).getUserID();
        Record3<UInteger, String, String> result = getWorkflowFromDatabase(workflowID);

        if (result == null) {
            throw new TexeraWebException("Workflow with id: " + workflowID + " does not exit.");
        }

        try {
            // the json string stored in USERWORKFLOW.WORKFLOWBODY correspond to the interface savedWorkflowBody
            // in new-gui/src/app/workspace/service/save-workflow/save-workflow.service.ts
            ObjectNode savedWorkflowBody = new ObjectMapper().readValue(result.get(WORKFLOW.CONTENT), ObjectNode.class);
            return new UserWorkflow(
                    result.get(WORKFLOW.WF_ID),
                    result.get(WORKFLOW.NAME),
                    savedWorkflowBody
            );
        } catch (IOException e) {
            throw new TexeraWebException(e.getMessage());
        }
    }

    /**
     * this method handles the frontend's request to save a specific workflow
     * at current design, it takes a workflowID and a JSON string representing the new workflow
     * it updates the corresponding mysql record; throws an error if the workflow does not exist
     * for future design, it should also take userID as an parameter.
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
            return new GenericWebResponse(1,"workflow " + workflowID + " does not exist in the database");
        }
        int result = updateWorkflowInDataBase(workflowID,workflowBody);
        throwErrorWhenNotOne("Error occurred while updating workflow to database",result);
        return GenericWebResponse.generateSuccessResponse();
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
     *
     * @param userID
     * @param workflowID
     * @param workflowName
     * @param content
     * @return
     */
    private int insertWorkflowToDataBase(String userID, UInteger workflowID, String workflowName, String content) {
        return UserSqlServer.createDSLContext().insertInto(WORKFLOW)
                // uncomment below to give workflows the concept of ownership
                // .set(USERWORKFLOW.USERID,userID)
                .set(WORKFLOW.WF_ID, workflowID)
                .set(WORKFLOW.NAME, workflowName)
                .set(WORKFLOW.CONTENT, content)
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
