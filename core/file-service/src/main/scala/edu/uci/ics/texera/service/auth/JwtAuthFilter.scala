package edu.uci.ics.texera.service.auth

import jakarta.ws.rs.container.{ContainerRequestContext, ContainerRequestFilter}
import jakarta.ws.rs.core.{HttpHeaders, SecurityContext}
import jakarta.ws.rs.ext.Provider
import jakarta.ws.rs.container.ResourceInfo
import jakarta.ws.rs.core.Context

import java.security.Principal
import com.typesafe.scalalogging.LazyLogging
import edu.uci.ics.texera.dao.jooq.generated.enums.UserRoleEnum

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
