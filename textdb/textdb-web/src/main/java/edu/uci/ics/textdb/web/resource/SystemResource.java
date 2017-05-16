package edu.uci.ics.textdb.web.resource;

import com.fasterxml.jackson.databind.ObjectMapper;
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
	public TextdbWebResponse getMetadata() throws Exception {
		List<TableMetadata> tableMetadata = RelationManager.getRelationManager().getMetaData();
		return new TextdbWebResponse(0, new ObjectMapper().writeValueAsString(tableMetadata));
	}
}