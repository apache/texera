package edu.uci.ics.amber.service

import jakarta.ws.rs.container.{
  ContainerRequestContext,
  ContainerResponseContext,
  ContainerResponseFilter
}
import jakarta.ws.rs.core.MultivaluedMap
import jakarta.ws.rs.ext.Provider

@Provider
class CORSFilter extends ContainerResponseFilter {
  override def filter(
      requestContext: ContainerRequestContext,
      responseContext: ContainerResponseContext
  ): Unit = {
    val headers: MultivaluedMap[String, AnyRef] = responseContext.getHeaders
    headers.add("Access-Control-Allow-Origin", "*")
    headers.add("Access-Control-Allow-Methods", "GET, POST, DELETE, PUT, OPTIONS")
    headers.add("Access-Control-Allow-Headers", "Authorization, Content-Type, Accept")
    headers.add("Access-Control-Allow-Credentials", "true")
  }
}
