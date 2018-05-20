package edu.uci.ics.texera.web;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import com.github.dirkraft.dropwizard.fileassets.FileAssetsBundle;

import edu.uci.ics.texera.api.exception.TexeraException;
import edu.uci.ics.texera.perftest.twitter.AsterixTwitterIngest;
import edu.uci.ics.texera.perftest.twitter.TwitterSample;
import edu.uci.ics.texera.storage.RelationManager;
import edu.uci.ics.texera.storage.TableMetadata;
import edu.uci.ics.texera.web.healthcheck.SampleHealthCheck;
import edu.uci.ics.texera.web.resource.DownloadFileResource;
import edu.uci.ics.texera.web.resource.FileUploadResource;
import edu.uci.ics.texera.web.resource.QueryPlanResource;
import edu.uci.ics.texera.web.resource.PlanStoreResource;
import edu.uci.ics.texera.web.resource.SystemResource;
import io.dropwizard.Application;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import org.eclipse.jetty.servlets.CrossOriginFilter;
import org.glassfish.jersey.media.multipart.MultiPartFeature;

import javax.servlet.DispatcherType;
import javax.servlet.FilterRegistration;
import java.util.EnumSet;
import java.util.List;

/**
 * This is the main application class from where the Texera application
 * will be launched, it is parametrized with the configuration
 * Created by kishore on 10/4/16.
 */
public class TexeraWebApplication extends Application<TexeraWebConfiguration> {

    @Override
    public void initialize(Bootstrap<TexeraWebConfiguration> bootstrap) {
        // serve static frontend GUI files
        bootstrap.addBundle(new FileAssetsBundle("./gui/", "/", "index.html"));
    }

    @Override
    public void run(TexeraWebConfiguration texeraWebConfiguration, Environment environment) throws Exception {
        // serve backend at /api
        environment.jersey().setUrlPattern("/api/*");
        
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

        // Creates an instance of the FileUploadResource class to register with Jersey
        final FileUploadResource fileUploadResource = new FileUploadResource();
        // Registers the fileUploadResource with Jersey
        environment.jersey().register(fileUploadResource);

        // Registers MultiPartFeature to support file upload
        environment.jersey().register(MultiPartFeature.class);

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
//        System.out.println("Writing twitter index");
//        TwitterSample.writeTwitterIndex();
//        System.out.println("Finished writing twitter index");
        
        System.out.println("deleting all old tables");
        
        List<TableMetadata> metadataList = RelationManager.getInstance().getMetaData();
        for (TableMetadata metadata: metadataList) {
            try {
                RelationManager.getInstance().deleteTable(metadata.getTableName());
            } catch (TexeraException e) {
                e.printStackTrace();
            }
        }
        
        System.out.println("finish deleting all old tables");

        
        System.out.println("Ingesting twitter");
        
        AsterixTwitterIngest.ingestKeywords("01_twitter_climate_change", "climate climatechange science pseudoscience policy evidence warming globalwarming consensus environment hoax", null, null);
        AsterixTwitterIngest.ingestKeywords("02_twitter_hurricane_maria", "hurricane huracan government gobierno disaster desastre FEMA Puerto Rico", null, null);
        
        AsterixTwitterIngest.ingestKeywords("11_twitter_police_violence", "PoliceBrutality PoliceViolence BlackLivesMatter PhilandoCastile AltonSterling LawEnforcement PoliceState BlueLivesMatter PoliceLivesMatter BackTheBlue AllLivesMatter", null, null);
        AsterixTwitterIngest.ingestKeywords("12_twitter_press_freedom", "FreedomOfThePress FreedomOfPress PressFreedom PressBlackOut FakeNews FakeNewsMedia Journalism FreedomOfSpeech FirstAmendment 1stAmendment FreeSpeech ProtectThePress censorship orwellian mediabias", null, null);
        AsterixTwitterIngest.ingestKeywords("13_twitter_gun_control", "guncontrol gunviolence guns NRA prayforvegas LasVegasShooting planoshooting OrlandoShooting SandyShooting BronxLebanonShooting CascadeMallShooting DallasPoliceShooting PikeCountyShooting gunownersofamerica BatonRougeshooting", null, null);
        AsterixTwitterIngest.ingestKeywords("14_twitter_nfl_protest", "TakeAKnee NFLBoycott BoycottTheNFL NFLBurnNotice TakeAKneeNFL NationalAnthem NoFansLeft TakeAStandNotAKnee TakeTheKnee KneelNFL", null, null);
        AsterixTwitterIngest.ingestKeywords("15_twitter_immigration_policy", "MuslimBan NoMuslimBan MuslimBanProtest TravelBan TNTweeters TimeIsNow RefugeeBan RefugeesWelcome BuildTheWall BuildThatWall TrumpWall MexicanWall NoBanNoWall NoRefugees RefugeesNotWelcome SecureTheBorder immigration immigrationreform deportillegals DACA EndDACA NoDACA dreamers HereToStay DreamAct immigrationpolicy", null, null);

 
        System.out.println("finished");
        
        new TexeraWebApplication().run(args);
    }
}