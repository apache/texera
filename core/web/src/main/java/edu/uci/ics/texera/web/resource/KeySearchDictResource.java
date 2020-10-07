package edu.uci.ics.texera.web.resource;

import edu.uci.ics.texera.dataflow.sqlServerInfo.UserSqlServer;
import edu.uci.ics.texera.web.TexeraWebException;
import edu.uci.ics.texera.web.response.GenericWebResponse;
import io.dropwizard.jersey.sessions.Session;
import org.apache.commons.lang3.tuple.Pair;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import org.glassfish.jersey.media.multipart.FormDataParam;
import org.jooq.Record4;
import org.jooq.Result;
import org.jooq.types.UInteger;

import javax.servlet.http.HttpSession;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static edu.uci.ics.texera.dataflow.jooq.generated.Tables.KEY_SEARCH_DICT;
import static org.jooq.impl.DSL.defaultValue;


@Path("/user/dictionary")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public class KeySearchDictResource {

    // limitation due to the default 'max_allowed_packet' variable is 4 mb in MySQL
    private static final long MAXIMUM_DICTIONARY_SIZE = (2 * 1024 * 1024); // 2 mb

    /**
     * Corresponds to `src/app/common/type/user-dictionary.ts`
     */
    public static class UserDictionary {
        public UInteger id; // the ID in MySQL database is unsigned int
        public String name;
        public List<String> items;
        public String description;
        
        public UserDictionary() {} // default constructor reserved for json

        public UserDictionary(UInteger id, String name, List<String> items, String description) {
            this.id = id;
            this.name = name;
            this.items = items;
            this.description = description;
        }
    }
    
    /**
     * Corresponds to `src/app/common/type/user-dictionary.ts`
     */
    public static class UserManualDictionary {
        public String name;
        public String content;
        public String separator;
        public String description;

        public UserManualDictionary() { } // default constructor reserved for json
        
        public boolean isValid() {
            return name != null && name.length() != 0 &&
                    content != null && content.length() != 0 &&
                    separator != null && description != null;
        }
    }
    
    @POST
    @Path("/upload-manual-dict")
    public GenericWebResponse uploadManualDictionary(
            @Session HttpSession session,
            UserManualDictionary userManualDictionary
    ) {
        if (userManualDictionary == null || !userManualDictionary.isValid()) {
            throw new TexeraWebException("Error occurred in user manual dictionary");
        }
        UInteger userID = UserResource.getUser(session).getUserID();
        if (userManualDictionary.separator.isEmpty()) {userManualDictionary.separator = ",";}
        
        List<String> itemArray = convertStringToList(
                userManualDictionary.content, 
                userManualDictionary.separator
                );
        byte[] contentByteArray = convertListToByteArray(itemArray);
        
        int count = insertDictionaryToDataBase(
                userManualDictionary.name, 
                contentByteArray,
                userManualDictionary.description,
                userID);
        
        throwErrorWhenNotOne("Error occurred while inserting dictionary to database", count);
        
        return GenericWebResponse.generateSuccessResponse();
    }
    
    @POST
    @Path("/upload")
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    public GenericWebResponse uploadDictionary(
            @Session HttpSession session,
            @FormDataParam("file") InputStream uploadedInputStream,
            @FormDataParam("file") FormDataContentDisposition fileDetail,
            @FormDataParam("size") String sizeString,
            @FormDataParam("description") String description
    ) {
        UInteger userID = UserResource.getUser(session).getUserID();
        String dictName = fileDetail.getFileName();
        String separator = ",";
        long size = parseStringToUInteger(sizeString).longValue();
        
        Pair<Integer, String> validationResult = validateDictionary(dictName, userID, size);
        if (validationResult.getLeft() != 0) {
            return new GenericWebResponse(validationResult.getLeft(), validationResult.getRight());
        }
        
        String content = readFileContent(uploadedInputStream);
        List<String> itemList = convertStringToList(content, separator);
        byte[] contentByteArray = convertListToByteArray(itemList);
        
        int count = insertDictionaryToDataBase(
                dictName,
                contentByteArray,
                description,
                userID);
        
        throwErrorWhenNotOne("Error occurred while inserting dictionary to database", count);
        
        return GenericWebResponse.generateSuccessResponse();
    }
    
    @GET
    @Path("/list")
    public List<UserDictionary> listUserDictionaries(
            @Session HttpSession session
    ) {
        UInteger userID = UserResource.getUser(session).getUserID();
        
        Result<Record4<UInteger, String, byte[], String>> result = getUserDictionaryRecord(userID);
        
        if (result == null) return new ArrayList<>();
        
        List<UserDictionary> dictionaryList = result.stream()
                .map(
                    record -> new UserDictionary(
                            record.get(KEY_SEARCH_DICT.KSD_ID),
                            record.get(KEY_SEARCH_DICT.NAME),
                            convertContentToList(record.get(KEY_SEARCH_DICT.CONTENT)),
                            record.get(KEY_SEARCH_DICT.DESCRIPTION)
                    )
                        ).collect(Collectors.toList());
        
        return dictionaryList;
    }
    
    @DELETE
    @Path("/delete/{dictID}")
    public GenericWebResponse deleteUserDictionary(
            @Session HttpSession session,
            @PathParam("dictID") String dictID
    ) {
        UInteger dictIdUInteger = parseStringToUInteger(dictID);
        UInteger userID = UserResource.getUser(session).getUserID();
        
        int count = deleteInDatabase(dictIdUInteger, userID);
        throwErrorWhenNotOne("delete dictionary " + dictIdUInteger + " failed in database", count);
        
        return GenericWebResponse.generateSuccessResponse();
    }
    
    @PUT
    @Path("/update")
    public GenericWebResponse updateUserDictionary(
            @Session HttpSession session,
            UserDictionary userDictionary
    ) {
        UInteger userID = UserResource.getUser(session).getUserID();
        
        int count = updateInDatabase(
                userDictionary,
                userID
                );
        
        throwErrorWhenNotOne("Error occurred while inserting dictionary to database", count);
        
        return GenericWebResponse.generateSuccessResponse();
    }
    
    @POST
    @Path("/validate")
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    public GenericWebResponse validateUserDictionary(
            @Session HttpSession session, 
            @FormDataParam("name") String fileName,
            @FormDataParam("size") String sizeString) {
        UInteger userID = UserResource.getUser(session).getUserID();
        long size = parseStringToUInteger(sizeString).longValue();
        
        Pair<Integer, String> validationResult = validateDictionary(fileName, userID, size);
        return new GenericWebResponse(validationResult.getLeft(), validationResult.getRight());
    }
    
    private Pair<Integer, String> validateDictionary(String dictName, UInteger userID, long size) {
        if (dictName == null) {
            return Pair.of(1, "dictionary name cannot be null");
        } else if (dictName.trim().isEmpty()) {
            return Pair.of(1, "dictionary name cannot be empty");
        } else if (!validateDictionarySize(size)) {
            return Pair.of(1, "dictionary is too large, maximun limit is " + MAXIMUM_DICTIONARY_SIZE/1024/1024 + " mb.");
        } else if (isDictionaryNameExisted(dictName, userID)){
            return Pair.of(1, "dictionary name already exists");
        } else {
            return Pair.of(0, "dictionary validation success");
        }
    }
    
    private boolean validateDictionarySize(long size) {
        return size < MAXIMUM_DICTIONARY_SIZE;
    }
    
    private boolean isDictionaryNameExisted(String dictName, UInteger userID) {
           return UserSqlServer.createDSLContext()
                    .fetchExists(
                            UserSqlServer.createDSLContext()
                                    .selectFrom(KEY_SEARCH_DICT)
                                    .where(KEY_SEARCH_DICT.UID.equal(userID)
                                            .and(KEY_SEARCH_DICT.NAME.equal(dictName)))
                            );
    }
    
    private int updateInDatabase(UserDictionary userDictionary, UInteger userID) {
            return UserSqlServer.createDSLContext()
                    .update(KEY_SEARCH_DICT)
                    .set(KEY_SEARCH_DICT.NAME, userDictionary.name)
                    .set(KEY_SEARCH_DICT.CONTENT, convertListToByteArray(userDictionary.items))
                    .set(KEY_SEARCH_DICT.DESCRIPTION, userDictionary.description)
                    .where(KEY_SEARCH_DICT.KSD_ID.eq(userDictionary.id).and(KEY_SEARCH_DICT.UID.eq(userID)))
                    .execute();
    }
    
    private int deleteInDatabase(UInteger dictID, UInteger userID) {
            return UserSqlServer.createDSLContext()
                    .delete(KEY_SEARCH_DICT)
                    .where(KEY_SEARCH_DICT.KSD_ID.eq(dictID).and(KEY_SEARCH_DICT.UID.eq(userID)))
                    .execute();
    }
    
    private Result<Record4<UInteger, String, byte[], String>> getUserDictionaryRecord(UInteger userID) {
            return UserSqlServer.createDSLContext()
                    .select(KEY_SEARCH_DICT.KSD_ID, KEY_SEARCH_DICT.NAME, KEY_SEARCH_DICT.CONTENT, KEY_SEARCH_DICT.DESCRIPTION)
                    .from(KEY_SEARCH_DICT)
                    .where(KEY_SEARCH_DICT.UID.equal(userID))
                    .fetch();
    }
    
    private int insertDictionaryToDataBase(String name, byte[] content, String description, UInteger userID) {
            return UserSqlServer.createDSLContext()
                    .insertInto(KEY_SEARCH_DICT)
                    .set(KEY_SEARCH_DICT.UID, userID)
                    .set(KEY_SEARCH_DICT.KSD_ID, defaultValue(KEY_SEARCH_DICT.KSD_ID))
                    .set(KEY_SEARCH_DICT.NAME, name)
                    .set(KEY_SEARCH_DICT.CONTENT, content)
                    .set(KEY_SEARCH_DICT.DESCRIPTION, description)
                    .execute();
    }
    
    /**
     * write the whole list into the byte array.
     * @param list
     * @return
     */
    private byte[] convertListToByteArray(List<String> list) {
            try(
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                ObjectOutputStream objOstream = new ObjectOutputStream(baos))
            {
                objOstream.writeObject(list);
                return baos.toByteArray();
            }
            catch (Exception e){
                throw new TexeraWebException("Error when converting list of dictionary items into byte array");
            }
    }
    
    /**
     * convert the input byte array to the list of string
     * the result list is converted back from byte array so it is unchecked.
     * @param content
     * @return
     */
    @SuppressWarnings("unchecked")
    private List<String> convertContentToList(byte[] content) {
        try(
                ByteArrayInputStream bais = new ByteArrayInputStream(content);
                ObjectInputStream ois = new ObjectInputStream(bais)){
            return (ArrayList<String>) ois.readObject();
        } catch (Exception e) {
            throw new TexeraWebException(e);
        }
    }
    
    private UInteger parseStringToUInteger(String userID) throws TexeraWebException {
        try {
            return UInteger.valueOf(userID);
        } catch (NumberFormatException e) {
            throw new TexeraWebException("Incorrect String to long");
        }
    }
    
    private String readFileContent(InputStream fileStream) {
        StringBuilder fileContents = new StringBuilder();
        String line;
        try (BufferedReader br = new BufferedReader(new InputStreamReader(fileStream))) {
            while ((line = br.readLine()) != null) {
                fileContents.append(line);
            }
        } catch (IOException e) {
            throw new TexeraWebException(e);
        }
        return fileContents.toString();
    }
    
    private List<String> convertStringToList(String content, String separator) {
        return Stream.of(
                    content.trim().split(separator)
                )
                .filter(s -> !s.isEmpty())
                .distinct()
                .collect(Collectors.toList());
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
