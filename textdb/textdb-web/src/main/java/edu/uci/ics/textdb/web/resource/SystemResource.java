package edu.uci.ics.textdb.web.resource;

import com.fasterxml.jackson.databind.ObjectMapper;
import edu.uci.ics.textdb.api.engine.Engine;
import edu.uci.ics.textdb.api.engine.Plan;
import edu.uci.ics.textdb.api.tuple.Tuple;
import edu.uci.ics.textdb.dataflow.sink.TupleStreamSink;
import edu.uci.ics.textdb.dataflow.utils.DataflowUtils;
import edu.uci.ics.textdb.storage.CatalogConstants;
import edu.uci.ics.textdb.storage.RelationManager;
import edu.uci.ics.textdb.storage.TableMetadata;
import edu.uci.ics.textdb.web.request.QueryPlanRequest;
import edu.uci.ics.textdb.web.response.TextdbWebResponse;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.List;

@Path("/metadata")
@Produces(MediaType.APPLICATION_JSON)
public class SystemResource {
    @GET
    public TextdbWebResponse getMetadata() throws Exception {
        ObjectMapper objectMapper = new ObjectMapper();

        List<TableMetadata> tableMetadata = RelationManager.getRelationManager().getMetaData();
        return new TextdbWebResponse(0, DataflowUtils.getMetadataJSON(tableMetadata).toString());
    }
}