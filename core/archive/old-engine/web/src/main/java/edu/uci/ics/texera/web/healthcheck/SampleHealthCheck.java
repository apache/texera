package edu.uci.ics.texera.web.healthcheck;


import com.codahale.metrics.health.HealthCheck;

/**
 * This is a sample HealthCheck created for the web module. These health checks can be monitored
 * on the admin port of an application in order to check the status of an application. HealthChecks are runtime
 * tests of a web application.
 * Created by kishorenarendran on 10/9/16.
 */
public class SampleHealthCheck extends HealthCheck{

    /**
     * This overrided function will contain the core logic for checking whether
     * the RESTful web interface is functioning properly
     * @return - An object of the class Result
     * @throws Exception
     */
    @Override
    protected Result check() throws Exception {
        // Perform some checks here, like database connection
        return Result.healthy();
    }
}
