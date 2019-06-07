package edu.uci.ics.texera.web.resource;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import edu.uci.ics.texera.dataflow.resource.dictionary.DictionaryManager;
import edu.uci.ics.texera.web.TexeraWebException;
import edu.uci.ics.texera.web.response.GenericWebResponse;

import org.glassfish.jersey.media.multipart.BodyPartEntity;
import org.glassfish.jersey.media.multipart.FormDataBodyPart;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import org.glassfish.jersey.media.multipart.FormDataMultiPart;
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

    /**
     * This method will handle the request to add / update a
     * 	dictionary in the Lucene database
     * 
     * @param dictID
     * @param userDictionary
     * @return
     */
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

    /**
     * This method will handle the request to delete a
     * 	dictionary instance in the Lucene database.
     * 
     * @param dictID
     * @return
     */
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


    /**
     * This method will handle the request to upload a single file.
     * 
     * @param uploadedInputStream
     * @param fileDetail
     * @return
     */
    @POST
    @Path("/upload-file")
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    public GenericWebResponse uploadDictionaryFile(
            @FormDataParam("file") InputStream uploadedInputStream,
            @FormDataParam("file") FormDataContentDisposition fileDetail) {

        String fileName = fileDetail.getFileName();
        this.handleDictionaryUpload(uploadedInputStream, fileName);

        return new GenericWebResponse(0, "success");
    }
    
    
    /**
     * This method will handle the request to upload multiple files
     * 
     * @param multiPart
     * @return
     */
    @POST
    @Path("upload-files")
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    public GenericWebResponse uploadDictionaryFiles (
    		FormDataMultiPart multiPart) {
    	
		List<FormDataBodyPart> formBodyParts = multiPart.getFields("files");

		for (int i = 0; i < formBodyParts.size(); i++) {
			BodyPartEntity bodyPartEntity = (BodyPartEntity) formBodyParts.get(i).getEntity();
			String fileName = formBodyParts.get(i).getContentDisposition().getFileName();
			InputStream fileStream = bodyPartEntity.getInputStream();
			this.handleDictionaryUpload(fileStream, fileName);
		}

    	return new GenericWebResponse(0, "success");
    }
    
    /**
     * This method will handle the single file upload to the Lucene database.
     * 
     * It will first read the dictionary contents from the input stream. Then,
     * 	it will remove duplicate entries and store its contents inside the Lucene
     * 	database based on the data we fetched from input stream.
     * 
     * @param fileStream
     * @param fileName
     */
    private void handleDictionaryUpload(InputStream fileStream, String fileName) {
        StringBuilder fileContents = new StringBuilder();
        String line;
        try (BufferedReader br = new BufferedReader(new InputStreamReader(fileStream))) {
            while ((line = br.readLine()) != null) {
                fileContents.append(line);
            }
        } catch (IOException e) {
            throw new TexeraWebException("Error occurred while uploading dictionary");
        }

        
        String contents = fileContents.toString();
        List<String> dictEntriesWithDup = Arrays.asList(contents.split(",")).stream().map(s -> s.trim())
        		.filter(s -> !s.isEmpty()).collect(Collectors.toList());
 
        Set<String> dictEntriesWithoutDup = new HashSet<String> (dictEntriesWithDup);
        List<String> dictEntries = new ArrayList<String> (dictEntriesWithoutDup);

        // save the dictionary
        DictionaryManager dictionaryManager = DictionaryManager.getInstance();
        String randomDictionaryID = "dictionary-" + UUID.randomUUID().toString();
        dictionaryManager.addDictionary(randomDictionaryID, entriesToJson(dictEntries), fileName, "");
    }


    /**
     * This method serializes dictionary entries to JSON string.
     * 
     * @param dictEntries
     * @return
     */
    private String entriesToJson(List<String> dictEntries) {
        try {
            return new ObjectMapper().writeValueAsString(dictEntries);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
    
    /**
     * This method deserializes JSON strings to a list of
     * 	dictionary entries.
     * 
     * @param entriesJson
     * @return
     */
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
