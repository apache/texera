package edu.uci.ics.texera.auth

import com.typesafe.scalalogging.LazyLogging
import edu.uci.ics.texera.dao.jooq.generated.enums.UserRoleEnum
import jakarta.ws.rs.container.{ContainerRequestContext, ContainerRequestFilter, ResourceInfo}
import jakarta.ws.rs.core.{Context, HttpHeaders, SecurityContext}
import jakarta.ws.rs.ext.Provider

import java.security.Principal

@Provider
class JwtAuthFilter extends ContainerRequestFilter with LazyLogging {

  @Context
  private var resourceInfo: ResourceInfo = _

  override def filter(requestContext: ContainerRequestContext): Unit = {
    val authHeader = requestContext.getHeaderString(HttpHeaders.AUTHORIZATION)

    if (authHeader != null && authHeader.startsWith("Bearer ")) {
      val token = authHeader.substring(7) // Remove "Bearer " prefix
      val userOpt = JwtParser.parseToken(token)

      if (userOpt.isPresent) {
        val user = userOpt.get()
        requestContext.setSecurityContext(new SecurityContext {
          override def getUserPrincipal: Principal = user
          override def isUserInRole(role: String): Boolean =
            user.isRoleOf(UserRoleEnum.valueOf(role))
          override def isSecure: Boolean = false
          override def getAuthenticationScheme: String = "Bearer"
        })
      } else {
        logger.warn("Invalid JWT: Unable to parse token")
      }
    }
  }
}
