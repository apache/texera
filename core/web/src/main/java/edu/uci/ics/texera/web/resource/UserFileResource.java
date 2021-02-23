package edu.uci.ics.texera.web.resource;

import edu.uci.ics.texera.dataflow.resource.file.FileManager;
import edu.uci.ics.texera.dataflow.sqlServerInfo.SqlServer;
import edu.uci.ics.texera.web.TexeraWebException;
import edu.uci.ics.texera.web.response.GenericWebResponse;
import io.dropwizard.jersey.sessions.Session;
import org.apache.commons.lang3.tuple.Pair;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import org.glassfish.jersey.media.multipart.FormDataParam;
import org.jooq.Record1;
import org.jooq.Record5;
import org.jooq.Result;
import org.jooq.types.UInteger;

import javax.servlet.http.HttpSession;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import java.io.InputStream;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static edu.uci.ics.texera.dataflow.jooq.generated.Tables.FILE;
import static org.jooq.impl.DSL.defaultValue;

@Path("/user/file")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public class UserFileResource {

    /**
     * Corresponds to `src/app/common/type/user-file.ts`
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
        UInteger userID = UserResource.getUser(session).getUserID();
        String fileName = fileDetail.getFileName();
        UInteger sizeUInteger = parseStringToUInteger(size);

        Pair<Boolean, String> validationResult = validateFileName(fileName, userID);
        if (!validationResult.getLeft()) {
            return new GenericWebResponse(1, validationResult.getRight());
        }

        this.handleFileUpload(uploadedInputStream, fileName, description, sizeUInteger, userID);
        return GenericWebResponse.generateSuccessResponse();
    }

    @GET
    @Path("/list")
    public List<UserFile> listUserFiles(@Session HttpSession session) {
        UInteger userID = UserResource.getUser(session).getUserID();

        Result<Record5<UInteger, String, String, String, UInteger>> result = getUserFileRecord(userID);

        if (result == null) return new ArrayList<>();

        List<UserFile> fileList = result.stream()
                .map(
                        record -> new UserFile(
                                record.get(FILE.FID),
                                record.get(FILE.NAME),
                                record.get(FILE.PATH),
                                record.get(FILE.DESCRIPTION),
                                record.get(FILE.SIZE)
                        )
                ).collect(Collectors.toList());

        return fileList;
    }

    @DELETE
    @Path("/delete/{fileID}")
    public GenericWebResponse deleteUserFile(@PathParam("fileID") String fileID, @Session HttpSession session) {
        UInteger userID = UserResource.getUser(session).getUserID();
        UInteger fileIdUInteger = parseStringToUInteger(fileID);
        Record1<String> result = deleteInDatabase(fileIdUInteger, userID);

        if (result == null) return new GenericWebResponse(1, "The file does not exist");

        String filePath = result.get(FILE.PATH);
        FileManager.getInstance().deleteFile(Paths.get(filePath));

        return GenericWebResponse.generateSuccessResponse();
    }

    @POST
    @Path("/validate")
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    public GenericWebResponse validateUserFile(@Session HttpSession session, @FormDataParam("name") String fileName) {
        UInteger userID = UserResource.getUser(session).getUserID();
        Pair<Boolean, String> validationResult = validateFileName(fileName, userID);
        return new GenericWebResponse(validationResult.getLeft() ? 0 : 1, validationResult.getRight());
    }

    private Record1<String> deleteInDatabase(UInteger fileID, UInteger userID) {
        /**
         * Known problem for jooq 3.x
         * delete...returning clause does not work properly
         * retrieve the filepath first, then delete it.
         */
        Record1<String> result = SqlServer.createDSLContext()
                .select(FILE.PATH)
                .from(FILE)
                .where(FILE.FID.eq(fileID).and(FILE.UID.equal(userID)))
                .fetchOne();

        int count = SqlServer.createDSLContext()
                .delete(FILE)
                .where(FILE.FID.eq(fileID).and(FILE.UID.equal(userID)))
                //.returning(USERFILE.FILEPATH) does not work
                .execute();

        throwErrorWhenNotOne("delete file " + fileID + " failed in database", count);

        return result;
    }

    private Result<Record5<UInteger, String, String, String, UInteger>> getUserFileRecord(UInteger userID) {
        return SqlServer.createDSLContext()
                .select(FILE.FID, FILE.NAME, FILE.PATH, FILE.DESCRIPTION, FILE.SIZE)
                .from(FILE)
                .where(FILE.UID.equal(userID))
                .fetch();
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
        return SqlServer.createDSLContext().insertInto(FILE)
                .set(FILE.UID, userID)
                .set(FILE.FID, defaultValue(FILE.FID))
                .set(FILE.NAME, fileName)
                .set(FILE.PATH, path)
                .set(FILE.DESCRIPTION, description)
                .set(FILE.SIZE, size)
                .execute();
    }

    private Pair<Boolean, String> validateFileName(String fileName, UInteger userID) {
        if (fileName == null) {
            return Pair.of(false, "file name cannot be null");
        } else if (fileName.trim().isEmpty()) {
            return Pair.of(false, "file name cannot be empty");
        } else if (isFileNameExisted(fileName, userID)) {
            return Pair.of(false, "file name already exists");
        } else {
            return Pair.of(true, "filename validation success");
        }
    }

    private Boolean isFileNameExisted(String fileName, UInteger userID) {
        return SqlServer.createDSLContext()
                .fetchExists(
                        SqlServer.createDSLContext()
                                .selectFrom(FILE)
                                .where(FILE.UID.equal(userID)
                                        .and(FILE.NAME.equal(fileName)))
                );
    }

    /**
     * Most the sql operation should only be executed once. eg. insertion, deletion.
     * this method will raise TexeraWebException when the input number is not one
     *
     * @param errorMessage the message displaying when raising the error
     * @param number       the number to be checked with one
     * @throws TexeraWebException
     */
    private void throwErrorWhenNotOne(String errorMessage, int number) throws TexeraWebException {
        if (number != 1) {
            throw new TexeraWebException(errorMessage);
        }
    }
}
