/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package edu.uci.ics.texera.web

import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.github.dirkraft.dropwizard.fileassets.FileAssetsBundle
import com.typesafe.scalalogging.LazyLogging
import edu.uci.ics.amber.config.StorageConfig
import edu.uci.ics.amber.engine.common.Utils
import edu.uci.ics.amber.util.ObjectMapperUtils
import edu.uci.ics.texera.config.UserSystemConfig
import edu.uci.ics.texera.auth.SessionUser
import edu.uci.ics.texera.dao.SqlServer
import edu.uci.ics.texera.web.auth.JwtAuth.setupJwtAuth
import edu.uci.ics.texera.web.resource._
import edu.uci.ics.texera.web.resource.auth.{AuthResource, GoogleAuthResource}
import edu.uci.ics.texera.web.resource.dashboard.DashboardResource
import edu.uci.ics.texera.web.resource.dashboard.admin.execution.AdminExecutionResource
import edu.uci.ics.texera.web.resource.dashboard.admin.user.AdminUserResource
import edu.uci.ics.texera.web.resource.dashboard.admin.settings.AdminSettingsResource
import edu.uci.ics.texera.web.resource.dashboard.hub.HubResource
import edu.uci.ics.texera.web.resource.dashboard.user.project.{
  ProjectAccessResource,
  ProjectResource,
  PublicProjectResource
}
import edu.uci.ics.texera.web.resource.dashboard.user.quota.UserQuotaResource
import edu.uci.ics.texera.web.resource.dashboard.user.workflow.{
  WorkflowAccessResource,
  WorkflowExecutionsResource,
  WorkflowResource,
  WorkflowVersionResource
}
import io.dropwizard.auth.AuthValueFactoryProvider
import io.dropwizard.setup.{Bootstrap, Environment}
import io.dropwizard.websockets.WebsocketBundle
import org.eclipse.jetty.server.session.SessionHandler
import org.eclipse.jetty.servlet.ErrorPageErrorHandler
import org.eclipse.jetty.websocket.server.WebSocketUpgradeFilter
import org.glassfish.jersey.server.filter.RolesAllowedDynamicFeature

import java.time.Duration

object TexeraWebApplication {

  def main(args: Array[String]): Unit = {

    // TODO: figure out a safety way of calling discardUncommittedChangesOfAllDatasets
    // Currently in kubernetes, multiple pods calling this function can result into thread competition
    // discardUncommittedChangesOfAllDatasets()

    // start web server
    new TexeraWebApplication().run(
      "server",
      Utils.amberHomePath
        .resolve("src")
        .resolve("main")
        .resolve("resources")
        .resolve("web-config.yml")
        .toString
    )
  }
}

class TexeraWebApplication
    extends io.dropwizard.Application[TexeraWebConfiguration]
    with LazyLogging {

  override def initialize(bootstrap: Bootstrap[TexeraWebConfiguration]): Unit = {
    // serve static frontend GUI files
    bootstrap.addBundle(new FileAssetsBundle("../gui/dist", "/", "index.html"))
    // add websocket bundle
    bootstrap.addBundle(new WebsocketBundle(classOf[CollaborationResource]))
    // register scala module to dropwizard default object mapper
    bootstrap.getObjectMapper.registerModule(DefaultScalaModule)
  }

  override def run(configuration: TexeraWebConfiguration, environment: Environment): Unit = {
    ObjectMapperUtils.warmupObjectMapperForOperatorsSerde()

    // serve backend at /api
    environment.jersey.setUrlPattern("/api/*")

    SqlServer.initConnection(
      StorageConfig.jdbcUrl,
      StorageConfig.jdbcUsername,
      StorageConfig.jdbcPassword
    )

    // redirect all 404 to index page, according to Angular routing requirements
    val eph = new ErrorPageErrorHandler
    eph.addErrorPage(404, "/")
    environment.getApplicationContext.setErrorHandler(eph)

    val webSocketUpgradeFilter =
      WebSocketUpgradeFilter.configureContext(environment.getApplicationContext)
    webSocketUpgradeFilter.getFactory.getPolicy.setIdleTimeout(Duration.ofHours(1).toMillis)
    environment.getApplicationContext.setAttribute(
      classOf[WebSocketUpgradeFilter].getName,
      webSocketUpgradeFilter
    )

    // register SessionHandler
    environment.jersey.register(classOf[SessionHandler])
    environment.servlets.setSessionHandler(new SessionHandler)

    environment.jersey.register(classOf[SystemMetadataResource])
    // environment.jersey().register(classOf[MockKillWorkerResource])

    environment.jersey.register(classOf[HealthCheckResource])

    setupJwtAuth(environment)

    environment.jersey.register(
      new AuthValueFactoryProvider.Binder[SessionUser](classOf[SessionUser])
    )
    environment.jersey.register(classOf[RolesAllowedDynamicFeature])

    environment.jersey.register(classOf[AuthResource])
    environment.jersey.register(classOf[GoogleAuthResource])
    environment.jersey.register(classOf[UserConfigResource])
    environment.jersey.register(classOf[AdminUserResource])
    environment.jersey.register(classOf[PublicProjectResource])
    environment.jersey.register(classOf[WorkflowAccessResource])
    environment.jersey.register(classOf[WorkflowResource])
    environment.jersey.register(classOf[HubResource])
    environment.jersey.register(classOf[WorkflowVersionResource])
    environment.jersey.register(classOf[ProjectResource])
    environment.jersey.register(classOf[ProjectAccessResource])
    environment.jersey.register(classOf[WorkflowExecutionsResource])
    environment.jersey.register(classOf[DashboardResource])
    environment.jersey.register(classOf[GmailResource])
    environment.jersey.register(classOf[AdminExecutionResource])
    environment.jersey.register(classOf[UserQuotaResource])
    environment.jersey.register(classOf[AdminSettingsResource])
    environment.jersey.register(classOf[AIAssistantResource])

    if (UserSystemConfig.isUserSystemEnabled) {
      AuthResource.createAdminUser()
    }
  }
}
