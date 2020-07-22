package edu.uci.ics.texera.web.resource;

import edu.uci.ics.texera.dataflow.sqlServerInfo.UserSqlServer;
import edu.uci.ics.texera.web.TexeraWebException;
import io.dropwizard.jersey.sessions.Session;
import org.jooq.Record1;
import org.jooq.Record3;

import javax.servlet.http.HttpSession;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import static edu.uci.ics.texera.dataflow.jooq.generated.Tables.USERWORKFLOW;

// uncomment and use below to give workflows the concept of ownership
// @Path("/user/workflow")
@Path("/workflow")
@Produces(MediaType.APPLICATION_JSON)
public class UserWorkflowResource {

    /**
     * Corresponds to `src/app/common/type/user-file.ts`
     */
    public static class UserWorkflow {
        public String id; // the ID in MySQL database is unsigned int
        public String name;
        public String body;

        public UserWorkflow(String id, String name, String body) {
            this.id = id;
            this.name = name;
            this.body = body;
        }
    }

    @GET
    @Path("/get/{workflowID}")
    public UserWorkflow getUserWorkflow(@PathParam("workflowID") String workflowID, @Session HttpSession session) {
        System.out.println("with in getUserWorkflow for " + workflowID);
//        UInteger userID = UserResource.getUser(session).getUserID();
        Record3<String, String, String> result = getWorkflowFromDatabase(workflowID);

        if (result == null) {
            throw new TexeraWebException("Workflow with id: " + workflowID + " does not exit.");
        }

        return new UserWorkflow(
                result.get(USERWORKFLOW.WORKFLOWID),
                result.get(USERWORKFLOW.NAME),
                result.get(USERWORKFLOW.WORKFLOWBODY)
        );
    }

    private Record3<String, String, String> getWorkflowFromDatabase(String workflowID) {
        return UserSqlServer.createDSLContext()
                .select(USERWORKFLOW.WORKFLOWID, USERWORKFLOW.NAME, USERWORKFLOW.WORKFLOWBODY)
                .from(USERWORKFLOW)
                .where(USERWORKFLOW.WORKFLOWID.eq(workflowID))
                .fetchOne();
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
}
