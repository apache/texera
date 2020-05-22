package edu.uci.ics.texera.web.resource;

import static org.jooq.impl.DSL.defaultValue;

import java.io.InputStream;
import java.nio.file.Paths;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import javax.servlet.http.HttpSession;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import org.apache.commons.lang3.tuple.Pair;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import org.glassfish.jersey.media.multipart.FormDataParam;
import org.jooq.DSLContext;
import org.jooq.Record1;
import org.jooq.Record5;
import org.jooq.Result;
import org.jooq.types.UInteger;

import static edu.uci.ics.texera.dataflow.jooq.generated.Tables.*;

import edu.uci.ics.texera.web.TexeraWebException;
import edu.uci.ics.texera.dataflow.resource.file.FileManager;
import edu.uci.ics.texera.dataflow.sqlServerInfo.UserSqlServer;
import edu.uci.ics.texera.web.response.GenericWebResponse;
import io.dropwizard.jersey.sessions.Session;

@Path("/user/file")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public class UserFileResource {
    
    /**
     * Corresponds to `src/app/dashboard/type/user-file.ts`
     */
    public static class UserFile {
        public UInteger id; // the ID in MySQL database is unsigned int
        public String name;
        public String path;
        public String description;
        public UInteger size; // the size in MySQL database is unsigned int

        public UserFile(UInteger id, String name, String path, String description, UInteger size) {
            this.id = id;
            this.name = name;
            this.path = path;
            this.description = description;
            this.size = size;
        }
    }
        
    /**
     * This method will handle the request to upload a single file.
     * 
     * @param uploadedInputStream
     * @param fileDetail
     * @return
     */
    @POST
    @Path("/upload")
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    public GenericWebResponse uploadFile(
            @FormDataParam("file") InputStream uploadedInputStream,
            @FormDataParam("file") FormDataContentDisposition fileDetail,
            @FormDataParam("size") String size,
            @FormDataParam("description") String description,
            @Session HttpSession session
            ) {
        UInteger userID = UserResource.getUserFromSession(session).getUserID();
        String fileName = fileDetail.getFileName();
        UInteger sizeUInteger = parseStringToUInteger(size);
        
        Pair<Boolean, String> validationResult = validateFileName(fileName, userID);
        if (!validationResult.getLeft()) {
            return new GenericWebResponse(1, validationResult.getRight());
        }
        
        this.handleFileUpload(uploadedInputStream, fileName, description, sizeUInteger, userID);
        return new GenericWebResponse(0, "success");
    }
    
    @GET
    @Path("/list")
    public List<UserFile> listUserFiles(@Session HttpSession session){
        UInteger userID = UserResource.getUserFromSession(session).getUserID();
        
        Result<Record5<UInteger, String, String, String, UInteger>> result = getUserFileRecord(userID);
        
        if (result == null) return new ArrayList<>();
        
        List<UserFile> fileList = result.stream()
                .map(
                    record -> new UserFile(
                            record.get(USERFILE.FILEID),
                            record.get(USERFILE.NAME),
                            record.get(USERFILE.PATH),
                            record.get(USERFILE.DESCRIPTION),
                            record.get(USERFILE.SIZE)
                            )
                        ).collect(Collectors.toList());
        
        return fileList;
    }
    
    @DELETE
    @Path("/delete/{fileID}")
    public GenericWebResponse deleteUserFile(@PathParam("fileID") String fileID, @Session HttpSession session) {
        UInteger userID = UserResource.getUserFromSession(session).getUserID();
        UInteger fileIdUInteger = parseStringToUInteger(fileID);
        Record1<String> result = deleteInDatabase(fileIdUInteger, userID);
        
        if (result == null) return new GenericWebResponse(1, "The file does not exist");
        
        String filePath = result.get(USERFILE.PATH);
        FileManager.getInstance().deleteFile(Paths.get(filePath));
        
        return new GenericWebResponse(0, "success");
    }
    
    @POST
    @Path("/validate")
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    public GenericWebResponse validateUserFile(@Session HttpSession session, @FormDataParam("name") String fileName) {
        UInteger userID = UserResource.getUserFromSession(session).getUserID();
        Pair<Boolean, String> validationResult = validateFileName(fileName, userID);
        return new GenericWebResponse(validationResult.getLeft() ? 0 : 1, validationResult.getRight());
    }
    
    private Record1<String> deleteInDatabase(UInteger fileID, UInteger userID) {
        // Connection is AutoCloseable so it will automatically close when it finishes.
        try (Connection conn = UserSqlServer.getConnection()) {
            DSLContext create = UserSqlServer.createDSLContext(conn);
            
            /**
             * Known problem for jooq 3.x
             * delete...returning clause does not work properly
             * retrieve the filepath first, then delete it.
             */
            Record1<String> result = create
                    .select(USERFILE.PATH)
                    .from(USERFILE)
                    .where(USERFILE.FILEID.eq(fileID).and(USERFILE.USERID.equal(userID)))
                    .fetchOne();
            
            int count = create
                    .delete(USERFILE)
                    .where(USERFILE.FILEID.eq(fileID).and(USERFILE.USERID.equal(userID)))
                    //.returning(USERFILE.FILEPATH) does not work
                    .execute();
            
            throwErrorWhenNotOne("delete file " + fileID + " failed in database", count);
            
            return result;
            
        } catch (Exception e) {
            throw new TexeraWebException(e);
        }
    }
    
    private Result<Record5<UInteger, String, String, String, UInteger>> getUserFileRecord(UInteger userID) {
        // Connection is AutoCloseable so it will automatically close when it finishes.
        try (Connection conn = UserSqlServer.getConnection()) {
            DSLContext create = UserSqlServer.createDSLContext(conn);
            
            Result<Record5<UInteger, String, String, String, UInteger>> result = create
                    .select(USERFILE.FILEID, USERFILE.NAME, USERFILE.PATH, USERFILE.DESCRIPTION, USERFILE.SIZE)
                    .from(USERFILE)
                    .where(USERFILE.USERID.equal(userID))
                    .fetch();
            
            return result;
            
        } catch (Exception e) {
            throw new TexeraWebException(e);
        }
    }
    
    private void handleFileUpload(InputStream fileStream, String fileName, String description, UInteger size, UInteger userID) {
        int count = insertFileToDataBase(
                fileName, 
                FileManager.getFilePath(userID.toString(), fileName).toString(),
                size,
                description,
                userID);
        
        throwErrorWhenNotOne("Error occurred while inserting file record to database", count);
        
        FileManager.getInstance().storeFile(fileStream, fileName, userID.toString());
    }
    
    private UInteger parseStringToUInteger(String userID) throws TexeraWebException {
        try {
            return UInteger.valueOf(userID);
        } catch (NumberFormatException e) {
            throw new TexeraWebException("Incorrect String to Double");
        }
    }
    
    
    private int insertFileToDataBase(String fileName, String path, UInteger size, String description, UInteger userID) {
        // Connection is AutoCloseable so it will automatically close when it finishes.
        try (Connection conn = UserSqlServer.getConnection()) {
            DSLContext create = UserSqlServer.createDSLContext(conn);
            
            int count = create.insertInto(USERFILE)
                    .set(USERFILE.USERID,userID)
                    .set(USERFILE.FILEID, defaultValue(USERFILE.FILEID))
                    .set(USERFILE.NAME, fileName)
                    .set(USERFILE.PATH, path)
                    .set(USERFILE.DESCRIPTION, description)
                    .set(USERFILE.SIZE, size)
                    .execute();
            
            return count;
            
        } catch (Exception e) {
            throw new TexeraWebException(e);
        }
    }
    
    private Pair<Boolean, String> validateFileName(String fileName, UInteger userID) {
        if (fileName == null) {
            return Pair.of(false, "file name cannot be null");
        } else if (fileName.trim().isEmpty()) {
            return Pair.of(false, "file name cannot be empty");
        } else if (isFileNameExisted(fileName, userID)){
            return Pair.of(false, "file name already exists");
        } else {
            return Pair.of(true, "filename validation success");
        }
    }
    
    private Boolean isFileNameExisted(String fileName, UInteger userID) {
        try (Connection conn = UserSqlServer.getConnection()) {
            DSLContext create = UserSqlServer.createDSLContext(conn);
            
            boolean result = create
                    .fetchExists(
                            create.selectFrom(USERFILE)
                            .where(USERFILE.USERID.equal(userID)
                                    .and(USERFILE.NAME.equal(fileName)))
                            );
            return result;
        } catch (Exception e) {
            throw new TexeraWebException(e);
        }
    }
    
    /**
     * Most the sql operation should only be executed once. eg. insertion, deletion.
     * this method will raise TexeraWebException when the input number is not one
     * @param errorMessage the message displaying when raising the error
     * @param number the number to be checked with one
     * @throws TexeraWebException
     */
    private void throwErrorWhenNotOne(String errorMessage, int number) throws TexeraWebException {
        if (number != 1) {
            throw new TexeraWebException(errorMessage);
        }
    }
}
