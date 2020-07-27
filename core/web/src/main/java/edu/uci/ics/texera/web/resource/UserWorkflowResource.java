package edu.uci.ics.texera.web.resource;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import edu.uci.ics.texera.dataflow.sqlServerInfo.UserSqlServer;
import edu.uci.ics.texera.web.TexeraWebException;
import edu.uci.ics.texera.web.response.GenericWebResponse;
import io.dropwizard.jersey.sessions.Session;
import org.glassfish.jersey.media.multipart.FormDataParam;
import org.jooq.Record3;

import javax.servlet.http.HttpSession;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;

import java.io.IOException;

import static edu.uci.ics.texera.dataflow.jooq.generated.Tables.USERWORKFLOW;

/**
 * This file handles various request related to saved-workflows.
 * It sends mysql queries to the MysqlDB regarding the UserWorkflow Table
 * The details of UserWorkflowTable can be found in /core/scripts/sql/texera_ddl.sql
 */

// uncomment and use below to give workflows the concept of ownership
// @Path("/user/workflow")
@Path("/workflow")
@Produces(MediaType.APPLICATION_JSON)
public class UserWorkflowResource {

    /**
     * This method handles the frontend's request to get a specific workflow to be displayed
     * at current design, it only takes the workflowID and searches within the database for the matching workflow
     * for future design, it should also take userID as an parameter.
     * @param workflowID
     * @param session
     * @return
     */
    @GET
    @Path("/get/{workflowID}")
    public ObjectNode getUserWorkflow(@PathParam("workflowID") String workflowID, @Session HttpSession session) {
        // uncomment below to link user with workflow
        // UInteger userID = UserResource.getUser(session).getUserID();
        Record3<String, String, String> result = getWorkflowFromDatabase(workflowID);

        if (result == null) {
            throw new TexeraWebException("Workflow with id: " + workflowID + " does not exit.");
        }

        try {
            // the json string stored in USERWORKFLOW.WORKFLOWBODY correspond to the interface savedWorkflow
            // in new-gui/src/app/workspace/service/save-workflow/save-workflow.service.ts
            ObjectNode savedWorkflow = new ObjectMapper().readValue(result.get(USERWORKFLOW.WORKFLOWBODY), ObjectNode.class);
            return savedWorkflow;
        } catch (IOException e) {
            throw new TexeraWebException(e.getMessage());
        }
    }

    /**
     * this method handles the frontend's request to save a specific workflow
     * at current design, it only takes the workflowID
     * for future design, it should also take userID as an parameter.
     * @param session
     * @param workflowID
     * @param workflowBody
     * @return
     */
    @POST
    @Path("/set-workflow")
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    public GenericWebResponse setUserWorkflow(
            @Session HttpSession session,
            @FormDataParam("workflowID") String workflowID,
            @FormDataParam("workflowBody") String workflowBody
    ) {

        int count = updateWorkflowInDataBase(workflowID,workflowBody);
        throwErrorWhenNotOne("Error occurred while inserting workflow to database",count);

        return GenericWebResponse.generateSuccessResponse();
    }



    private Record3<String, String, String> getWorkflowFromDatabase(String workflowID) {
        return UserSqlServer.createDSLContext()
                .select(USERWORKFLOW.WORKFLOWID, USERWORKFLOW.NAME, USERWORKFLOW.WORKFLOWBODY)
                .from(USERWORKFLOW)
                .where(USERWORKFLOW.WORKFLOWID.eq(workflowID))
                .fetchOne();
    }

    private int updateWorkflowInDataBase(String workflowID, String workflowBody) {
        return UserSqlServer.createDSLContext().update(USERWORKFLOW)
                .set(USERWORKFLOW.WORKFLOWBODY, workflowBody)
                .where(USERWORKFLOW.WORKFLOWID.eq(workflowID))
                .execute();
    }

    private int insertWorkflowToDataBase(String userID, String workflowID, String workflowName, String workflowBody) {
        return UserSqlServer.createDSLContext().insertInto(USERWORKFLOW)
                // uncomment below to give workflows the concept of ownership
//                .set(USERWORKFLOW.USERID,userID)
                .set(USERWORKFLOW.WORKFLOWID, workflowID)
                .set(USERWORKFLOW.NAME, workflowName)
                .set(USERWORKFLOW.WORKFLOWBODY, workflowBody)
                .execute();
    }

    /**
     * Most the sql operation should only be executed once. eg. insertion, deletion.
     * this method will raise TexeraWebException when the input number is not one
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