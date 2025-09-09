package edu.uci.ics.texera

import edu.uci.ics.texera.service.resource.AccessControlResource
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.mockito.Mockito._
import jakarta.ws.rs.core.{HttpHeaders, MultivaluedHashMap, Response, UriInfo}
import java.net.URI
import java.util

class AccessControlResourceSpec extends AnyFlatSpec with Matchers {

  "AccessControlResource" should "return FORBIDDEN for a GET request without a token" in {
    val mockUriInfo = mock(classOf[UriInfo])
    val mockHttpHeaders = mock(classOf[HttpHeaders])
    val queryParams = new MultivaluedHashMap[String, String]()
    queryParams.add("cuid", "1")
    val requestHeaders = new MultivaluedHashMap[String, String]()

    when(mockUriInfo.getQueryParameters).thenReturn(queryParams)
    when(mockUriInfo.getRequestUri).thenReturn(new URI("http://localhost:8080/auth/some/path"))
    when(mockHttpHeaders.getRequestHeaders).thenReturn(requestHeaders)
    when(mockHttpHeaders.getRequestHeader("Authorization")).thenReturn(new util.ArrayList[String]())

    val accessControlResource = new AccessControlResource()
    val response = accessControlResource.authorizeGet(mockUriInfo, mockHttpHeaders)

    response.getStatus shouldBe Response.Status.FORBIDDEN.getStatusCode
  }

  it should "return FORBIDDEN for a GET request with a non-integer cuid" in {
    val mockUriInfo = mock(classOf[UriInfo])
    val mockHttpHeaders = mock(classOf[HttpHeaders])
    val queryParams = new MultivaluedHashMap[String, String]()
    queryParams.add("cuid", "abc")
    val requestHeaders = new MultivaluedHashMap[String, String]()
    requestHeaders.add("Authorization", "Bearer dummy-token")

    when(mockUriInfo.getQueryParameters).thenReturn(queryParams)
    when(mockUriInfo.getRequestUri).thenReturn(new URI("http://localhost:8080/auth/some/path"))
    when(mockHttpHeaders.getRequestHeaders).thenReturn(requestHeaders)
    when(mockHttpHeaders.getRequestHeader("Authorization")).thenReturn(util.Arrays.asList("Bearer dummy-token"))

    val accessControlResource = new AccessControlResource()
    val response = accessControlResource.authorizeGet(mockUriInfo, mockHttpHeaders)

    response.getStatus shouldBe Response.Status.FORBIDDEN.getStatusCode
  }

  it should "return FORBIDDEN for a POST request without a token" in {
    val mockUriInfo = mock(classOf[UriInfo])
    val mockHttpHeaders = mock(classOf[HttpHeaders])
    val queryParams = new MultivaluedHashMap[String, String]()
    queryParams.add("cuid", "1")
    val requestHeaders = new MultivaluedHashMap[String, String]()

    when(mockUriInfo.getQueryParameters).thenReturn(queryParams)
    when(mockUriInfo.getRequestUri).thenReturn(new URI("http://localhost:8080/auth/some/path"))
    when(mockHttpHeaders.getRequestHeaders).thenReturn(requestHeaders)
    when(mockHttpHeaders.getRequestHeader("Authorization")).thenReturn(new util.ArrayList[String]())

    val accessControlResource = new AccessControlResource()
    val response = accessControlResource.authorizePost(mockUriInfo, mockHttpHeaders)

    response.getStatus shouldBe Response.Status.FORBIDDEN.getStatusCode
  }


}