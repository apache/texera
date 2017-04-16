package edu.uci.ics.textdb.web;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;

import edu.uci.ics.textdb.api.engine.Plan;
import edu.uci.ics.textdb.api.exception.TextDBException;
import edu.uci.ics.textdb.api.tuple.Tuple;
import edu.uci.ics.textdb.dataflow.sink.TupleStreamSink;
import edu.uci.ics.textdb.perftest.sample.SampleExtraction;
import edu.uci.ics.textdb.plangen.LogicalPlan;
import edu.uci.ics.textdb.web.healthcheck.SampleHealthCheck;
import edu.uci.ics.textdb.web.request.beans.KeywordSourceBean;
import edu.uci.ics.textdb.web.request.beans.NlpExtractorBean;
import edu.uci.ics.textdb.web.request.beans.TupleStreamSinkBean;
import edu.uci.ics.textdb.web.resource.InitSystemResource;
import edu.uci.ics.textdb.web.resource.PlanStoreResource;
import edu.uci.ics.textdb.web.resource.QueryPlanResource;
import io.dropwizard.Application;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import org.eclipse.jetty.servlets.CrossOriginFilter;

import javax.servlet.DispatcherType;
import javax.servlet.FilterRegistration;
import java.util.EnumSet;
import java.util.List;

/**
 * This is the main application class from where the TextDB application
 * will be launched, it is parametrized with the configuration
 * Created by kishore on 10/4/16.
 */
public class TextdbWebApplication extends Application<TextdbWebConfiguration> {


    // Defining some simple operators for a simple query plan to trigger Stanford NLP loading
    private static final KeywordSourceBean KEYWORD_SOURCE_BEAN = new KeywordSourceBean("KeywordSource_0", "KeywordSource",
            "content", "100", "0", "Cleide Moreira, Director of Epidemiological Surveillance of SESAU", "conjunction",
            "promed");
    private static final NlpExtractorBean NLP_EXTRACTOR_BEAN = new NlpExtractorBean("NlpExtractor_0", "NlpExtractor",
            "content", "100", "0", "location");
    private static final TupleStreamSinkBean TUPLE_STREAM_SINK_BEAN = new TupleStreamSinkBean("TupleStreamSink_0",
            "TupleStreamSink", "content", "100", "0");

    @Override
    public void initialize(Bootstrap<TextdbWebConfiguration> bootstrap) {
        // Will have some initialization information here
    }

    @Override
    public void run(TextdbWebConfiguration textdbWebConfiguration, Environment environment) throws Exception {
        // Creates an instance of the QueryPlanResource class to register with Jersey
        final QueryPlanResource queryPlanResource = new QueryPlanResource();
        // Registers the QueryPlanResource with Jersey
        environment.jersey().register(queryPlanResource);

        // Creates an instance of the PlanStoreResource class to register with Jersey
        final PlanStoreResource planStoreResource = new PlanStoreResource();
        // Registers the PlanStoreResource with Jersey
        environment.jersey().register(planStoreResource);

        // Creates an instance of the InitSystemResource class to register with Jersey
        final InitSystemResource initSystemResource = new InitSystemResource();
        // Registers the InitSystemResource with Jersey
        environment.jersey().register(initSystemResource);

        // Creates an instance of the HealthCheck and registers it with the environment
        final SampleHealthCheck sampleHealthCheck = new SampleHealthCheck();
        // Registering the SampleHealthCheck with the environment
        environment.healthChecks().register("sample", sampleHealthCheck);

        // Configuring the object mapper used by Dropwizard
        environment.getObjectMapper().configure(MapperFeature.USE_GETTERS_AS_SETTERS, false);
        environment.getObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

        // Enable CORS headers
        final FilterRegistration.Dynamic cors =
                environment.servlets().addFilter("CORS", CrossOriginFilter.class);
        // Configure CORS parameters
        cors.setInitParameter("allowedOrigins", "*");
        cors.setInitParameter("allowedHeaders", "X-Requested-With,Content-Type,Accept,Origin");
        cors.setInitParameter("allowedMethods", "OPTIONS,GET,PUT,POST,DELETE,HEAD");
        // Add URL mapping
        cors.addMappingForUrlPatterns(EnumSet.allOf(DispatcherType.class), true, "/*");
    }

    private static void loadStanfordNLP() throws TextDBException{

        // Creating a simple logical plan with a Stanford NLP Extractor operator
        LogicalPlan logicalPlan = new LogicalPlan();
        logicalPlan.addOperator(KEYWORD_SOURCE_BEAN.getOperatorID(), KEYWORD_SOURCE_BEAN.getOperatorType(),
                KEYWORD_SOURCE_BEAN.getOperatorProperties());
        logicalPlan.addOperator(NLP_EXTRACTOR_BEAN.getOperatorID(), NLP_EXTRACTOR_BEAN.getOperatorType(),
                NLP_EXTRACTOR_BEAN.getOperatorProperties());
        logicalPlan.addOperator(TUPLE_STREAM_SINK_BEAN.getOperatorID(), TUPLE_STREAM_SINK_BEAN.getOperatorType(),
                TUPLE_STREAM_SINK_BEAN.getOperatorProperties());
        logicalPlan.addLink(KEYWORD_SOURCE_BEAN.getOperatorID(), NLP_EXTRACTOR_BEAN.getOperatorID());
        logicalPlan.addLink(NLP_EXTRACTOR_BEAN.getOperatorID(), TUPLE_STREAM_SINK_BEAN.getOperatorID());

        // Triggering the execution of the above query plan
        Plan plan = logicalPlan.buildQueryPlan();
        TupleStreamSink sink = (TupleStreamSink) plan.getRoot();
        sink.open();
        List<Tuple> results = sink.collectAllTuples();
        sink.close();
    }

    public static void main(String args[]) throws Exception {
        System.out.println("Writing Sample Index");
        SampleExtraction.writeSampleIndex();
        System.out.println("Completed Writing Sample Index");
        System.out.println("Started Loading Stanford NLP");
        loadStanfordNLP();
        System.out.println("Finished Loading Stanford NLP");
        new TextdbWebApplication().run(args);
    }
}