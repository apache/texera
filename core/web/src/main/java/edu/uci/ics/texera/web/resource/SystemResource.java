package edu.uci.ics.texera.web.resource;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import edu.uci.ics.texera.api.constants.DataConstants;
import edu.uci.ics.texera.api.exception.StorageException;
import edu.uci.ics.texera.dataflow.common.JsonSchemaHelper;
import edu.uci.ics.texera.dataflow.common.OperatorGroupConstants;
import edu.uci.ics.texera.dataflow.common.OperatorGroupConstants.GroupOrder;
import edu.uci.ics.texera.dataflow.common.PredicateBase;
import edu.uci.ics.texera.storage.RelationManager;
import edu.uci.ics.texera.storage.TableMetadata;
import edu.uci.ics.texera.web.TexeraWebException;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

@Path("/resources")
@Produces(MediaType.APPLICATION_JSON)
public class SystemResource {
    
    private ObjectMapper objectMapper = DataConstants.defaultObjectMapper;
    
    public static class OperatorMetadata {
        @JsonProperty("operators")
        List<JsonNode> operatorSchemaList;
        @JsonProperty("groups")
        List<GroupOrder> operatorGroupList;
        
        public OperatorMetadata() { }
        
        public OperatorMetadata(List<JsonNode> operatorSchemaList, List<GroupOrder> operatorGroupList) {
            this.operatorSchemaList = operatorSchemaList;
            this.operatorGroupList = operatorGroupList;
        }
    }
    
    @GET
    @Path("/operator-metadata")
    public OperatorMetadata getOperatorMetadata() {
        try {
             
            List<JsonNode> operators = new ArrayList<>();
            for (Class<? extends PredicateBase> predicateClass : JsonSchemaHelper.operatorTypeMap.keySet()) {
                JsonNode schemaNode = objectMapper.readTree(
                        Files.readAllBytes(JsonSchemaHelper.getJsonSchemaPath(predicateClass)));
                operators.add(schemaNode);
            }
            
            OperatorMetadata operatorMetadata = new OperatorMetadata(operators, OperatorGroupConstants.OperatorGroupOrderList);
            
            return operatorMetadata;
        } catch (Exception e) {
            throw new TexeraWebException(e);
        }
    }
    
	@GET
	@Path("/table-metadata")
	public List<TableMetadata> getMetadata() throws StorageException, JsonProcessingException {
		List<TableMetadata> tableMetadata = RelationManager.getInstance().getMetaData();
		return tableMetadata;
	}

}