package web.resources

import jakarta.ws.rs.core.MediaType
import jakarta.ws.rs.{GET, Path, Produces}

@Path("/hello-world")
@Produces(Array(MediaType.APPLICATION_JSON))
class HelloWorldResource {
  @GET
  def sayHello(): String = {
    "Hello, Dropwizard with Scala!"
  }
}
