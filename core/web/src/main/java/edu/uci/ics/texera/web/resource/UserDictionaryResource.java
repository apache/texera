package edu.uci.ics.texera.web.resource;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import edu.uci.ics.texera.dataflow.resource.dictionary.DictionaryManager;
import edu.uci.ics.texera.web.TexeraWebException;
import edu.uci.ics.texera.web.response.GenericWebResponse;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import org.glassfish.jersey.media.multipart.FormDataParam;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

@Path("/users/dictionaries/")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public class UserDictionaryResource {

    /**
     * Corresponds to `app/dashboard/service/user-dictionary/user-dictionary.interface.ts`
     */
    public static class UserDictionary {
        public String id;
        public String name;
        public List<String> items;
        public String description;

        public UserDictionary() { }

        public UserDictionary(String id, String name, List<String> items, String description) {
            this.id = id;
            this.name = name;
            this.items = items;
            this.description = description;
        }
    }

    /**
     * Get the list of dictionary IDs
     */
    @GET
    public List<UserDictionary> getDictionaries() {
        DictionaryManager dictionaryManager = DictionaryManager.getInstance();
        List<UserDictionary> dictionaries = dictionaryManager.getDictionaryIDs().stream()
                .map(dictID -> new UserDictionary(dictID, dictionaryManager.getDictionaryName(dictID), 
                		this.entriesFromJson(dictionaryManager.getDictionaryContent(dictID)), 
                		dictionaryManager.getDictionaryDescription(dictID)))
                .collect(Collectors.toList());
        
        return dictionaries;
    }

    /**
     * Get the content of dictionary
     */
    @GET
    @Path("/{dictionaryID}")
    public UserDictionary getDictionary(@PathParam("dictionaryID") String dictID) {
        try {
            DictionaryManager dictionaryManager = DictionaryManager.getInstance();
            String dictionaryContent = dictionaryManager.getDictionaryContent(dictID);

            ObjectMapper objectMapper = new ObjectMapper();
            List<String> dictEntries = objectMapper.readValue(dictionaryContent,
                    objectMapper.getTypeFactory().constructCollectionType(List.class, String.class));

            return new UserDictionary(dictID, dictionaryManager.getDictionaryName(dictID), dictEntries, 
            		dictionaryManager.getDictionaryDescription(dictID));
        } catch (Exception e) {
            throw new TexeraWebException(e);
        }
    }

    @PUT
    @Path("/{dictionaryID}")
    public GenericWebResponse putDictionary(
            @PathParam("dictionaryID") String dictID,
            UserDictionary userDictionary
    ) {
        DictionaryManager dictionaryManager = DictionaryManager.getInstance();
        List<String> dictIDs = dictionaryManager.getDictionaryIDs();
        if (dictIDs.contains(dictID)) {
            dictionaryManager.deleteDictionary(dictID);
        }
        
        dictionaryManager.addDictionary(dictID, entriesToJson(userDictionary.items), userDictionary.name, userDictionary.description);

        return new GenericWebResponse(0, "success");
    }

    @DELETE
    @Path("/{dictionaryID}")
    public GenericWebResponse deleteDictionary(
            @PathParam("dictionaryID") String dictID
    ) {
        DictionaryManager dictionaryManager = DictionaryManager.getInstance();
        List<String> dictIDs = dictionaryManager.getDictionaryIDs();
        if (dictIDs.contains(dictID)) {
            dictionaryManager.deleteDictionary(dictID);
        }
        return new GenericWebResponse(0, "success");
    }


    @POST
    @Path("/upload-file")
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    public GenericWebResponse uploadDictionaryFile(
            @FormDataParam("file") InputStream uploadedInputStream,
            @FormDataParam("file") FormDataContentDisposition fileDetail) {

        StringBuilder fileContents = new StringBuilder();
        String line;
        try (BufferedReader br = new BufferedReader(new InputStreamReader(uploadedInputStream))) {
            while ((line = br.readLine()) != null) {
                fileContents.append(line);
            }
        } catch (IOException e) {
            throw new TexeraWebException("Error occurred while uploading dictionary");
        }

        String fileName = fileDetail.getFileName();
        
        String contents = fileContents.toString();
        List<String> dictEntriesWithDup = Arrays.asList(contents.split(",")).stream().map(s -> s.trim()).filter(s -> !s.isEmpty()).collect(Collectors.toList());
 
        Set<String> dictEntriesWithoutDup = new HashSet<String> (dictEntriesWithDup);
        List<String> dictEntries = new ArrayList<String> (dictEntriesWithoutDup);

        // save the dictionary
        DictionaryManager dictionaryManager = DictionaryManager.getInstance();
        String randomDictionaryID = "dictionary-" + UUID.randomUUID().toString();
        dictionaryManager.addDictionary(randomDictionaryID, entriesToJson(dictEntries), fileName, "");

        return new GenericWebResponse(0, "success");
    }


    private String entriesToJson(List<String> dictEntries) {
        try {
            return new ObjectMapper().writeValueAsString(dictEntries);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private List<String> entriesFromJson(String entriesJson) {
        try {
            ObjectMapper objectMapper = new ObjectMapper();
            return objectMapper.readValue(entriesJson,
                    objectMapper.getTypeFactory().constructCollectionType(List.class, String.class));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }



}
