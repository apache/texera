package edu.uci.ics.texera

import edu.uci.ics.texera.auth.JwtAuth
import edu.uci.ics.texera.dao.MockTexeraDB
import edu.uci.ics.texera.dao.jooq.generated.enums.UserRoleEnum
import edu.uci.ics.texera.dao.jooq.generated.tables.daos.UserDao
import edu.uci.ics.texera.dao.jooq.generated.tables.pojos.User
import edu.uci.ics.texera.service.resource.AccessControlResource
import jakarta.ws.rs.core.{HttpHeaders, MultivaluedHashMap, Response, UriInfo}
import org.mockito.Mockito._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

import java.net.URI
import java.util

class AccessControlResourceSpec extends AnyFlatSpec
  with Matchers
  with BeforeAndAfterAll
  with BeforeAndAfterEach
  with MockTexeraDB {

  private val testUser: User = {
    val user = new User()
    user.setUid(1)
    user.setName("testuser")
    user.setEmail("test@example.com")
    user.setRole(UserRoleEnum.REGULAR)
    user.setPassword("password")
    user
  }

  private var token: String = _

  override protected def beforeAll(): Unit = {
    initializeDBAndReplaceDSLContext()
    val userDao = new UserDao(getDSLContext.configuration())
    userDao.insert(testUser)
    val claims = JwtAuth.jwtClaims(testUser, 1)
    token = JwtAuth.jwtToken(claims)
  }

  override protected def afterAll(): Unit = {
    shutdownDB()
  }

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

  "AccessControlResource" should "return FORBIDDEN when user does not have access to the computing unit" in {
    // Mock the request context
    val mockUriInfo = mock(classOf[UriInfo])
    val mockHttpHeaders = mock(classOf[HttpHeaders])

    // Prepare query parameters with a computing unit ID (cuid)
    val queryParams = new MultivaluedHashMap[String, String]()
    queryParams.add("cuid", "1") // Assuming user 1 does not have access to cuid 1

    // Prepare request headers with the generated JWT
    val requestHeaders = new MultivaluedHashMap[String, String]()
    requestHeaders.add("Authorization", "Bearer " + token)

    // Stub the mock objects to return the prepared data
    when(mockUriInfo.getQueryParameters).thenReturn(queryParams)
    when(mockUriInfo.getRequestUri).thenReturn(new URI("http://localhost:8080/auth/some/path"))
    when(mockHttpHeaders.getRequestHeaders).thenReturn(requestHeaders)
    when(mockHttpHeaders.getRequestHeader("Authorization")).thenReturn(util.Arrays.asList("Bearer " + token))

    // Instantiate the resource and call the method under test
    val accessControlResource = new AccessControlResource()
    val response = accessControlResource.authorizeGet(mockUriInfo, mockHttpHeaders)

    // Assert that the response status is FORBIDDEN
    response.getStatus shouldBe Response.Status.FORBIDDEN.getStatusCode
  }
}