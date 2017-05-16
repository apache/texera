package edu.uci.ics.textdb.web;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import com.github.dirkraft.dropwizard.fileassets.FileAssetsBundle;

import edu.uci.ics.textdb.perftest.sample.SampleExtraction;
import edu.uci.ics.textdb.perftest.twitter.TwitterSample;
import edu.uci.ics.textdb.web.healthcheck.SampleHealthCheck;
import edu.uci.ics.textdb.web.resource.NewQueryPlanResource;
import edu.uci.ics.textdb.web.resource.PlanStoreResource;
import edu.uci.ics.textdb.web.resource.SystemResource;
import io.dropwizard.Application;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import org.eclipse.jetty.servlets.CrossOriginFilter;

import javax.servlet.DispatcherType;
import javax.servlet.FilterRegistration;
import java.util.EnumSet;

/**
 * This is the main application class from where the TextDB application
 * will be launched, it is parametrized with the configuration
 * Created by kishore on 10/4/16.
 */
public class TextdbWebApplication extends Application<TextdbWebConfiguration> {

    @Override
    public void initialize(Bootstrap<TextdbWebConfiguration> bootstrap) {
        // serve static frontend GUI files
        bootstrap.addBundle(new FileAssetsBundle("./textdb-angular-gui/", "/", "index.html"));
    }

    @Override
    public void run(TextdbWebConfiguration textdbWebConfiguration, Environment environment) throws Exception {
        // serve backend at /api
        environment.jersey().setUrlPattern("/api/*");
        
        final NewQueryPlanResource newQueryPlanResource = new NewQueryPlanResource();
        environment.jersey().register(newQueryPlanResource);

        // Creates an instance of the PlanStoreResource class to register with Jersey
        final PlanStoreResource planStoreResource = new PlanStoreResource();
        // Registers the PlanStoreResource with Jersey
        environment.jersey().register(planStoreResource);

        // Creates an instance of the HealthCheck and registers it with the environment
        final SampleHealthCheck sampleHealthCheck = new SampleHealthCheck();
        // Registering the SampleHealthCheck with the environment
        environment.healthChecks().register("sample", sampleHealthCheck);

        // Creates an instance of the InitSystemResource class to register with Jersey
        final SystemResource systemResource = new SystemResource();
        // Registers the systemResource with Jersey
        environment.jersey().register(systemResource);

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


    public static void main(String args[]) throws Exception {
        System.out.println("Writing promed Index");
        SampleExtraction.writeSampleIndex();
        System.out.println("Finished Writing promed Index");
        System.out.println("Writing twitter index");
        TwitterSample.writeTwitterIndex();
        System.out.println("Finished writing twitter index");
        new TextdbWebApplication().run(args);
    }
}