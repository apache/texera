package edu.uci.ics.texera.web.resource;

import edu.uci.ics.texera.api.exception.TexeraException;
import edu.uci.ics.texera.dataflow.resource.file.FileManager;
import edu.uci.ics.texera.dataflow.sqlServerInfo.UserSqlServer;
import edu.uci.ics.texera.web.TexeraWebException;
import edu.uci.ics.texera.web.response.GenericWebResponse;
import io.dropwizard.jersey.sessions.Session;
import org.jooq.Record1;
import org.jooq.Record5;
import org.jooq.Result;
import org.jooq.types.UInteger;

import javax.servlet.http.HttpSession;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static edu.uci.ics.texera.dataflow.jooq.generated.Tables.USERFILE;
import static edu.uci.ics.texera.dataflow.jooq.generated.Tables.USERWORKFLOW;
import static org.jooq.impl.DSL.defaultValue;

@Path("/user/workflow")
public class UserWorkflowResource {

    /**
     * Corresponds to `src/app/common/type/user-file.ts`
     */
    public static class UserWorkflow {
        public UInteger id; // the ID in MySQL database is unsigned int
        public String name;
        public String body;

        public UserWorkflow(UInteger id, String name, String body) {
            this.id = id;
            this.name = name;
            this.body = body;
        }
    }

    @GET
    @Path("/get/{workflowID}")
    public UserWorkflow getUserWorkflow(@PathParam("workflowID") String workflowID, @Session HttpSession session) {
        System.out.println("with in getUserWorkflow for " + workflowID);
        UInteger userID = UserResource.getUser(session).getUserID();
        UInteger workflowIDUInteger = parseStringToUInteger(workflowID);
        Record1<String> result = getWorkflowFromDatabase(workflowIDUInteger, userID);

        if (result == null) {
            throw new TexeraWebException("Workflow with id: " + workflowID + " does not exit.");
        }

        return new UserWorkflow(
                result.get(USERWORKFLOW.WORKFLOWID),
                result.get(USERWORKFLOW.NAME),
                result.get(USERWORKFLOW.WORKFLOWBODY)
        );
    }

    private Record1<String> getWorkflowFromDatabase(UInteger workflowID, UInteger userID) {
        return UserSqlServer.createDSLContext()
                .select(USERWORKFLOW.WORKFLOWBODY)
                .from(USERWORKFLOW)
                .where(USERWORKFLOW.WORKFLOWID.eq(workflowID).and(USERWORKFLOW.USERID.equal(userID)))
                .fetchOne();
    }

    private int insertWorkflowToDataBase(UInteger userID, UInteger workflowID, String workflowName, String workflowBody) {
        return UserSqlServer.createDSLContext().insertInto(USERWORKFLOW)
                .set(USERWORKFLOW.USERID,userID)
                .set(USERWORKFLOW.WORKFLOWID, workflowID)
                .set(USERWORKFLOW.NAME, workflowName)
                .set(USERWORKFLOW.WORKFLOWBODY, workflowBody)
                .execute();
    }

    private UInteger parseStringToUInteger(String workflowID) throws TexeraWebException {
        try {
            return UInteger.valueOf(workflowID);
        } catch (NumberFormatException e) {
            throw new TexeraWebException("Incorrect String to Double");
        }
    }
}
