package edu.uci.ics.textdb.web;

import edu.uci.ics.textdb.web.healthcheck.SampleHealthCheck;
import edu.uci.ics.textdb.web.resource.QueryPlanResource;
import edu.uci.ics.textdb.web.resource.SampleResource;
import io.dropwizard.Application;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;

/**
 * This is the main application class from where the TextDB application
 * will be launched, it is parametrized with the configuration
 * Created by kishore on 10/4/16.
 */
public class TextdbWebApplication extends Application<TextdbWebConfiguration> {

    @Override
    public void initialize(Bootstrap<TextdbWebConfiguration> bootstrap) {
        // Will have some initialization information here
    }

    @Override
    public void run(TextdbWebConfiguration textdbWebConfiguration, Environment environment) throws Exception {
        // Creates an instance of the SampleResource class to register with Jersey
        final SampleResource sampleResource = new SampleResource();
        // Registers the SampleResource with Jersey
        environment.jersey().register(sampleResource);
        // Creates an instance of the QueryPlanResource class to register with Jersey
        final QueryPlanResource queryPlanResource = new QueryPlanResource();
        // Registers the QueryPlanResource with Jersey
        environment.jersey().register(queryPlanResource);
        // Creates an instance of the HealthCheck and registers it with the environment
        final SampleHealthCheck sampleHealthCheck = new SampleHealthCheck();
        // Registering the SampleHealthCheck with the environment
        environment.healthChecks().register("sample", sampleHealthCheck);
    }

    public static void main(String args[]) throws Exception {
        new TextdbWebApplication().run(args);
    }
}
