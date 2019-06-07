package edu.uci.ics.texera.web.resource;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import edu.uci.ics.texera.api.exception.TexeraException;
import edu.uci.ics.texera.api.tuple.Tuple;
import edu.uci.ics.texera.dataflow.plangen.LogicalPlan;
import edu.uci.ics.texera.dataflow.planstore.PlanStore;
import edu.uci.ics.texera.dataflow.planstore.PlanStoreConstants;
import edu.uci.ics.texera.storage.DataReader;
import edu.uci.ics.texera.web.TexeraWebException;
import edu.uci.ics.texera.web.response.GenericWebResponse;
import edu.uci.ics.texera.web.response.planstore.QueryPlanBean;
import edu.uci.ics.texera.web.response.planstore.QueryPlanListBean;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import java.io.IOException;
import java.util.ArrayList;

/**
 * Created by kishorenarendran on 2/24/17.
 */
@Path("/planstore")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public class PlanStoreResource {

    private static PlanStore planStore;
    private static ObjectMapper mapper;

    static {
        try {
            planStore = PlanStore.getInstance();
            mapper = new ObjectMapper();
            mapper.configure(MapperFeature.USE_GETTERS_AS_SETTERS, false);
            mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
            planStore.createPlanStore();
        }
        catch (TexeraException e) {
            e.printStackTrace();
        }
    }

    @GET
    public QueryPlanListBean getAllQueryPlans() {
        ArrayList<QueryPlanBean> queryPlans = new ArrayList<>();

        try {
            // Getting an iterator for the plan store
            DataReader reader = planStore.getPlanIterator();
            reader.open();

            // Iterating through the stored plans, and mapping them to a QueryPlanRequest object
            Tuple tuple;
            while ((tuple = reader.getNextTuple()) != null) {
                String name = tuple.getField(PlanStoreConstants.NAME).getValue().toString();
                String description = tuple.getField(PlanStoreConstants.DESCRIPTION).getValue().toString();
                String logicalPlanJson = tuple.getField(PlanStoreConstants.LOGICAL_PLAN_JSON).getValue().toString();
                queryPlans.add(new QueryPlanBean(name, description,
                        mapper.readValue(logicalPlanJson, LogicalPlan.class)));
            }
        }
        catch(TexeraException e) {
            throw new TexeraWebException(e.getMessage());
        }
        catch(IOException e) {
            throw new TexeraWebException("fail to parse json:\n" + e.getMessage());
        }
        return new QueryPlanListBean(queryPlans);
    }

    @GET
    @Path("/{plan_name}")
    public QueryPlanBean getQueryPlan(@PathParam("plan_name") String planName) {
        try {
            Tuple tuple = planStore.getPlan(planName);
            if(tuple == null) {
                throw new TexeraWebException("Plan with the given name does not exist");
            }
            QueryPlanBean queryPlanBean = new QueryPlanBean(tuple.getField(PlanStoreConstants.NAME).getValue().toString(),
                    tuple.getField(PlanStoreConstants.DESCRIPTION).getValue().toString(),
                    mapper.readValue(tuple.getField(PlanStoreConstants.LOGICAL_PLAN_JSON).getValue().toString(), LogicalPlan.class));
            return queryPlanBean;
        }
        catch(TexeraException e) {
            throw new TexeraWebException(e.getMessage());
        }
        catch(IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    @POST
    public GenericWebResponse addQueryPlan(String queryPlanBeanJson) {
        try {
            QueryPlanBean queryPlanBean = new ObjectMapper().readValue(queryPlanBeanJson, QueryPlanBean.class);
            // Adding the query plan to the PlanStore
            planStore.addPlan(queryPlanBean.getName(), queryPlanBean.getDescription(),
                    mapper.writeValueAsString(queryPlanBean.getQueryPlan()));
        }
        catch(TexeraException e) {
            throw new TexeraWebException(e.getMessage());
        }
        catch(IOException e) {
            throw new TexeraWebException(e.getMessage());
        }
        return new GenericWebResponse(0, "Success");
    }

    @DELETE
    @Path("/{plan_name}")
    public GenericWebResponse deleteQueryPlan(@PathParam("plan_name") String planName) {
        try {
            // Deleting the plan from the plan store
            planStore.deletePlan(planName);
        }
        catch(TexeraException e) {
            throw new TexeraWebException(e.getMessage());
        }
        return new GenericWebResponse(0, "Success");
    }

    @PUT
    @Path("/{plan_name}")
    public GenericWebResponse updateQueryPlan(@PathParam("plan_name") String planName, String queryPlanBeanJson) {
        try {
            QueryPlanBean queryPlanBean = new ObjectMapper().readValue(queryPlanBeanJson, QueryPlanBean.class);
            // Updating the plan in the plan store
            planStore.updatePlan(planName, queryPlanBean.getDescription(),
                    mapper.writeValueAsString(queryPlanBean.getQueryPlan()));
        }
        catch(IOException | TexeraException e) {
            throw new TexeraWebException(e.getMessage());
        }
        return new GenericWebResponse(0, "Success");
    }
}