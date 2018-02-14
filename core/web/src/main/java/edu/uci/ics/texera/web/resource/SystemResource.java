package edu.uci.ics.texera.web.resource;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;

import edu.uci.ics.texera.api.constants.DataConstants;
import edu.uci.ics.texera.api.exception.StorageException;
import edu.uci.ics.texera.dataflow.common.PredicateBase;
import edu.uci.ics.texera.dataflow.jsonschema.JsonSchemaHelper;
import edu.uci.ics.texera.dataflow.resource.dictionary.DictionaryManager;
import edu.uci.ics.texera.storage.RelationManager;
import edu.uci.ics.texera.storage.TableMetadata;
import edu.uci.ics.texera.web.TexeraWebException;
import edu.uci.ics.texera.web.response.TexeraWebResponse;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;

import java.nio.file.Files;
import java.util.List;

@Path("/resources")
@Produces(MediaType.APPLICATION_JSON)
public class SystemResource {
    
    private ObjectMapper objectMapper = DataConstants.defaultObjectMapper;
    
    @GET
    @Path("/operator-metadata")
    public JsonNode getOperatorMetadata() {
        try {
            ArrayNode allOperatorSchema = objectMapper.createArrayNode();
            for (Class<? extends PredicateBase> predicateClass : JsonSchemaHelper.operatorTypeMap.keySet()) {
                JsonNode schemaNode = objectMapper.readTree(
                        Files.readAllBytes(JsonSchemaHelper.getJsonSchemaPath(predicateClass)));
                allOperatorSchema.add(schemaNode);
            }
            return allOperatorSchema;
        } catch (Exception e) {
            throw new TexeraWebException(e);
        }
    }
    
	@GET
	@Path("/table-metadata")
	public TexeraWebResponse getMetadata() throws StorageException, JsonProcessingException {
		List<TableMetadata> tableMetadata = RelationManager.getInstance().getMetaData();
		return new TexeraWebResponse(0, new ObjectMapper().writeValueAsString(tableMetadata));
	}

    /**
     * Get the list of dictionaries
     */
	@GET
	@Path("/dictionaries")
	public TexeraWebResponse getDictionaries() throws StorageException, JsonProcessingException {
		DictionaryManager dictionaryManager = DictionaryManager.getInstance();
		List<String> dictionaries = dictionaryManager.getDictionaries();

		return new TexeraWebResponse(0, new ObjectMapper().writeValueAsString(dictionaries));
	}

	 /**
     * Get the content of dictionary
     */
	@GET
	@Path("/dictionary")
	public TexeraWebResponse getDictionary(@QueryParam("name") String name) {
        DictionaryManager dictionaryManager = DictionaryManager.getInstance();
        String dictionaryContent = dictionaryManager.getDictionary(name);

		return new TexeraWebResponse(0, dictionaryContent);
	}

}