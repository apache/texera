package edu.uci.ics.texera.web.resource;

import edu.uci.ics.texera.dataflow.jooq.generated.tables.records.UseraccountRecord;
import edu.uci.ics.texera.dataflow.sqlServerInfo.UserSqlServer;
import edu.uci.ics.texera.web.TexeraWebException;
import edu.uci.ics.texera.web.response.GenericWebResponse;
import io.dropwizard.jersey.sessions.Session;

import org.apache.commons.lang3.tuple.Pair;
import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.Record1;
import org.jooq.types.UInteger;

import javax.servlet.http.HttpSession;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import java.sql.Connection;

import static edu.uci.ics.texera.dataflow.jooq.generated.Tables.USERACCOUNT;
import static org.jooq.impl.DSL.defaultValue;


@Path("/users/")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public class UserResource {
    private static final String SESSION_USER = "texera-user";
    
    /**
     * Corresponds to `src/app/common/type/user.ts`
     */
    public static class User {
        public String userName;
        public UInteger userID; // the ID in MySQL database is unsigned int
        
        public static User generateErrorAccount() {
            return new User("", UInteger.valueOf(0));
        }
        
        public User(String userName, UInteger userID) {
            this.userName = userName;
            this.userID = userID;
        }
        
        public String getUserName() {
            return userName;
        }

        public UInteger getUserID() {
            return userID;
        }
    }

    public static class UserRegistrationRequest {
        public String userName;
    }

    public static class UserLoginRequest {
        public String userName;
    }
    
    /**
     * Corresponds to `src/app/common/type/user.ts`
     */
    public static class UserWebResponse {
        public int code; // 0 represents success and 1 represents error
        public User user;
        public String message;
        
        public static UserWebResponse generateErrorResponse(String message) {
            return new UserWebResponse(1, User.generateErrorAccount(), message);
        }
        
        public static UserWebResponse generateSuccessResponse(User user) {
            return new UserWebResponse(0, user, null);
        }

        private UserWebResponse(int code, User user, String message) {
            this.code = code;
            this.user = user;
            this.message = message;
        }
    }
    
    public static User getUser(HttpSession session) {
        return (User) session.getAttribute(SESSION_USER);
    }

    private static void setUser(HttpSession session, User user) {
        session.setAttribute(SESSION_USER, user);
    }

    @GET
    @Path("/auth/status")
    public UserWebResponse authStatus(@Session HttpSession session) {
        User user = getUser(session);
        if (user == null) {
            return UserWebResponse.generateErrorResponse("");
        } else {
            return UserWebResponse.generateSuccessResponse(user);
        }
    }

    @POST
    @Path("/login")
    public UserWebResponse login(@Session HttpSession session, UserLoginRequest request) {
        String userName = request.userName;
        Condition loginCondition = USERACCOUNT.USERNAME.equal(userName);
        Record1<UInteger> result = getUserID(loginCondition);

        if (result == null) { // not found
            return UserWebResponse.generateErrorResponse("username/password is incorrect");
        }

        User user = new User(userName, result.get(USERACCOUNT.USERID));
        setUserSession(session, user);

        return UserWebResponse.generateSuccessResponse(user);
    }

    @POST
    @Path("/register")
    public UserWebResponse register(@Session HttpSession session, UserRegistrationRequest request) {
        String userName = request.userName;
        Pair<Boolean, String> validationResult = validateUsername(userName);
        if (!validationResult.getLeft()) {
            return UserWebResponse.generateErrorResponse(validationResult.getRight());
        }

        Condition registerCondition = USERACCOUNT.USERNAME.equal(userName);
        Record1<UInteger> result = getUserID(registerCondition);

        if (result != null) {
            return UserWebResponse.generateErrorResponse("Username already exists");
        }

        UseraccountRecord returnID = insertUserAccount(userName);
        User user = new User(userName, returnID.get(USERACCOUNT.USERID));
        setUserSession(session, user);

        return UserWebResponse.generateSuccessResponse(user);
    }
    
    @GET
    @Path("/logout")
    public GenericWebResponse logOut(@Session HttpSession session) {
        setUserSession(session, null);
        return GenericWebResponse.generateSuccessResponse();
    }
    
    private Record1<UInteger> getUserID(Condition condition) {
        // Connection is AutoCloseable so it will automatically close when it finishes.
        try (Connection conn = UserSqlServer.getConnection()) {
            DSLContext create = UserSqlServer.createDSLContext(conn);
            
            Record1<UInteger> result = create
                    .select(USERACCOUNT.USERID)
                    .from(USERACCOUNT)
                    .where(condition)
                    .fetchOne();
            return result;
        } catch (Exception e) {
            throw new TexeraWebException(e);
        }
    }
    
    private UseraccountRecord insertUserAccount(String userName) {
        // Connection is AutoCloseable so it will automatically close when it finishes.
        try (Connection conn = UserSqlServer.getConnection()) {
            DSLContext create = UserSqlServer.createDSLContext(conn);
            
            UseraccountRecord result = create.insertInto(USERACCOUNT)
                    .set(USERACCOUNT.USERNAME, userName)
                    .set(USERACCOUNT.USERID, defaultValue(USERACCOUNT.USERID))
                    .returning(USERACCOUNT.USERID)
                    .fetchOne();
            
            return result;
            
        } catch (Exception e) {
            throw new TexeraWebException(e);
        }
    }
    
    private Pair<Boolean, String> validateUsername(String userName) {
        if (userName == null) {
            return Pair.of(false, "username cannot be null");
        } else if (userName.trim().isEmpty()) {
            return Pair.of(false, "username cannot be empty");
        } else {
            return Pair.of(true, "username validation success");
        }
    }
    
    private void setUserSession(HttpSession session, User user) {
        session.setAttribute(SESSION_USER, user);
    }
}
