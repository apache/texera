package edu.uci.ics.textdb.web.resource;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import edu.uci.ics.textdb.api.exception.StorageException;
import edu.uci.ics.textdb.exp.resource.dictionary.DictionaryManager;
import edu.uci.ics.textdb.storage.RelationManager;
import edu.uci.ics.textdb.storage.TableMetadata;
import edu.uci.ics.textdb.web.response.TextdbWebResponse;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import java.util.List;

@Path("/resources")
@Produces(MediaType.APPLICATION_JSON)
public class SystemResource {
	@GET
	@Path("/metadata")
	public TextdbWebResponse getMetadata() throws StorageException, JsonProcessingException {
		List<TableMetadata> tableMetadata = RelationManager.getRelationManager().getMetaData();
		return new TextdbWebResponse(0, new ObjectMapper().writeValueAsString(tableMetadata));
	}

    /**
     * Get the list of dictionaries
     */
	@GET
	@Path("/dictionaries")
	public TextdbWebResponse getDictionaries() throws StorageException, JsonProcessingException {
		DictionaryManager dictionaryManager = DictionaryManager.getInstance();
		List<String> dictionaries = dictionaryManager.getDictionaries();

		return new TextdbWebResponse(0, new ObjectMapper().writeValueAsString(dictionaries));
	}

	 /**
     * Get the content of dictionary
     */
	@GET
	@Path("/dictionary")
	public TextdbWebResponse getDictionary(@QueryParam("name") String name) {
        DictionaryManager dictionaryManager = DictionaryManager.getInstance();
        String dictionaryContent = dictionaryManager.getDictionary(name);

		return new TextdbWebResponse(0, dictionaryContent);
	}

}