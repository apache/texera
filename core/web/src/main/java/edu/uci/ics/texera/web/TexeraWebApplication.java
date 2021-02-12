package edu.uci.ics.texera.web;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import com.github.dirkraft.dropwizard.fileassets.FileAssetsBundle;
import edu.uci.ics.texera.api.utils.Utils;
import edu.uci.ics.texera.perftest.sample.SampleExtraction;
import edu.uci.ics.texera.perftest.twitter.TwitterSample;
import edu.uci.ics.texera.web.healthcheck.SampleHealthCheck;
import edu.uci.ics.texera.web.resource.*;
import io.dropwizard.Application;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import org.eclipse.jetty.server.session.SessionHandler;
import org.eclipse.jetty.servlet.ErrorPageErrorHandler;
import org.glassfish.jersey.media.multipart.MultiPartFeature;

/**
 * This is the main application class from where the Texera application
 * will be launched, it is parametrized with the configuration
 * Created by kishore on 10/4/16.
 */
public class TexeraWebApplication extends Application<TexeraWebConfiguration> {

    @Override
    public void initialize(Bootstrap<TexeraWebConfiguration> bootstrap) {
        // serve static frontend GUI files
        bootstrap.addBundle(new FileAssetsBundle("./new-gui/dist/", "/", "index.html"));
    }

    public static void main(String[] args) throws Exception {
        System.out.println("Writing promed Index");
        SampleExtraction.writeSampleIndex();
        System.out.println("Finished Writing promed Index");
        System.out.println("Writing twitter index");
        TwitterSample.writeTwitterIndex();
        System.out.println("Finished writing twitter index");

        String server = args.length > 1 ? args[0] : "server";
        String config = args.length > 2 ? args[1] :
                Utils.getTexeraHomePath().resolve("conf").resolve("web-config.yml").toString();

        new TexeraWebApplication().run(server, config);
    }

    @Override
    public void run(TexeraWebConfiguration texeraWebConfiguration, Environment environment) throws Exception {
        // serve backend at /api
        environment.jersey().setUrlPattern("/api/*");

        // redirect all 404 to index page, according to Angular routing requirements
        ErrorPageErrorHandler eph = new ErrorPageErrorHandler();
        eph.addErrorPage(404, "/");
        environment.getApplicationContext().setErrorHandler(eph);

        final QueryPlanResource newQueryPlanResource = new QueryPlanResource();
        environment.jersey().register(newQueryPlanResource);

        // Creates an instance of the PlanStoreResource class to register with Jersey
        final PlanStoreResource planStoreResource = new PlanStoreResource();
        // Registers the PlanStoreResource with Jersey
        environment.jersey().register(planStoreResource);

        final DownloadFileResource downloadFileResource = new DownloadFileResource();
        environment.jersey().register(downloadFileResource);

        // Creates an instance of the HealthCheck and registers it with the environment
        final SampleHealthCheck sampleHealthCheck = new SampleHealthCheck();
        // Registering the SampleHealthCheck with the environment
        environment.healthChecks().register("sample", sampleHealthCheck);

        // Creates an instance of the InitSystemResource class to register with Jersey
        final SystemResource systemResource = new SystemResource();
        // Registers the systemResource with Jersey
        environment.jersey().register(systemResource);

        environment.jersey().register(SessionHandler.class);
        environment.servlets().setSessionHandler(new SessionHandler());
        final UserResource userResource = new UserResource();
        environment.jersey().register(userResource);

        final UserFileResource userFileResource = new UserFileResource();
        environment.jersey().register(userFileResource);

        final KeywordDictionaryResource keywordDictionaryResource = new KeywordDictionaryResource();
        environment.jersey().register(keywordDictionaryResource);

        final WorkflowResource workflowResource = new WorkflowResource();
        environment.jersey().register(workflowResource);

        // Registers MultiPartFeature to support file upload
        environment.jersey().register(MultiPartFeature.class);

        // Configuring the object mapper used by Dropwizard
        environment.getObjectMapper().configure(MapperFeature.USE_GETTERS_AS_SETTERS, false);
        environment.getObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    }
}
