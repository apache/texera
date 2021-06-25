# Web

This is a RESTful API for the Texera project. The RESTful API module has been created using [Dropwizard](http://www.dropwizard.io/1.0.2/docs/). In order to
build and run the web module locally, execute the following commands.

## Building the module

Build with tests

`mvn clean install`

In order to skip the tests while building

`mvn clean install -DskipTests`

## Running the Texera RESTful Web Service Locally

`java -jar target/web-1.0-SNAPSHOT.jar server sample-config.yml`

The sample configuration `.yaml` file can be found in the root of the web module.
