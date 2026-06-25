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

package org.apache.texera.service

import com.fasterxml.jackson.databind.module.SimpleModule
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.typesafe.scalalogging.LazyLogging
import io.dropwizard.configuration.{EnvironmentVariableSubstitutor, SubstitutingSourceProvider}
import io.dropwizard.core.Application
import io.dropwizard.core.setup.{Bootstrap, Environment}
import org.apache.texera.common.config.StorageConfig
import org.apache.texera.amber.core.storage.util.LakeFSStorageClient
import org.apache.texera.auth.{AuthFeatures, RequestLoggingFilter, RoleAnnotationEnforcer}
import org.apache.texera.dao.SqlServer
import org.apache.texera.service.`type`.DatasetFileNode
import org.apache.texera.service.`type`.serde.DatasetFileNodeSerializer
import org.apache.texera.service.resource.{
  DatasetAccessResource,
  DatasetResource,
  HealthCheckResource
}
import org.apache.texera.service.util.S3StorageClient
import org.apache.texera.service.util.LargeBinaryManager
import org.eclipse.jetty.server.session.SessionHandler
import java.nio.file.Path

class FileService extends Application[FileServiceConfiguration] with LazyLogging {
  override def initialize(bootstrap: Bootstrap[FileServiceConfiguration]): Unit = {
    // enable environment variable substitution in YAML config
    bootstrap.setConfigurationSourceProvider(
      new SubstitutingSourceProvider(
        bootstrap.getConfigurationSourceProvider,
        new EnvironmentVariableSubstitutor(false)
      )
    )
    // Register Scala module to Dropwizard default object mapper
    bootstrap.getObjectMapper.registerModule(DefaultScalaModule)

    // register a new custom module just for DatasetFileNode serde/deserde
    val customSerializerModule = new SimpleModule("CustomSerializers")
    customSerializerModule.addSerializer(classOf[DatasetFileNode], new DatasetFileNodeSerializer())
    bootstrap.getObjectMapper.registerModule(customSerializerModule)
  }

  override def run(configuration: FileServiceConfiguration, environment: Environment): Unit = {
    // Serve backend at /api
    environment.jersey.setUrlPattern("/api/*")
    SqlServer.initConnection(
      StorageConfig.jdbcUrl,
      StorageConfig.jdbcUsername,
      StorageConfig.jdbcPassword
    )

    // check if the texera dataset bucket exists, if not create it
    awaitDependency("texera dataset bucket") {
      S3StorageClient.createBucketIfNotExist(StorageConfig.lakefsBucketName)
    }
    // ensure the large-binary S3 bucket exists before any workflow execution attempts to use it
    awaitDependency("large-binary bucket") {
      S3StorageClient.createBucketIfNotExist(LargeBinaryManager.DEFAULT_BUCKET)
    }
    // check if we can connect to the lakeFS service
    LakeFSStorageClient.healthCheck()

    environment.jersey.register(classOf[SessionHandler])
    environment.servlets.setSessionHandler(new SessionHandler)

    environment.jersey.register(classOf[HealthCheckResource])

    AuthFeatures.register(environment)

    environment.jersey.register(classOf[DatasetResource])
    environment.jersey.register(classOf[DatasetAccessResource])

    RoleAnnotationEnforcer.enforce(environment.jersey.getResourceConfig, "FileService")

    // Route request logs through SLF4J, controlled by TEXERA_SERVICE_LOG_LEVEL
    RequestLoggingFilter.register(environment.getApplicationContext)
  }

  /**
    * Runs `operation`, retrying with exponential backoff until it succeeds or `maxAttempts` is
    * reached, to tolerate a slow-to-start object store. The last failure is rethrown as the cause.
    * `sleep` is injectable for tests. Defaults: 6 attempts from 200ms (200, 400, 800, 1600, 3200), ~6s.
    */
  private[service] def awaitDependency(
      description: String,
      maxAttempts: Int = 6,
      initialDelayMillis: Long = 200L,
      sleep: Long => Unit = Thread.sleep
  )(operation: => Unit): Unit = {
    // Restore the interrupt status and fail fast rather than retrying, whether the
    // interrupt arrives while running `operation` or while sleeping between attempts.
    def failInterrupted(ie: InterruptedException): Nothing = {
      Thread.currentThread().interrupt()
      throw new RuntimeException(s"Interrupted while waiting for $description", ie)
    }

    var attempt = 1
    var delayMillis = initialDelayMillis
    while (true) {
      try {
        operation
        return
      } catch {
        case ie: InterruptedException => failInterrupted(ie)
        case e: Exception =>
          if (attempt >= maxAttempts) {
            throw new RuntimeException(
              s"$description not ready after $maxAttempts attempts: ${e.getMessage}",
              e
            )
          }
          logger.warn(
            s"$description not ready (attempt $attempt/$maxAttempts): ${e.getMessage}. " +
              s"Retrying in ${delayMillis}ms..."
          )
          try {
            sleep(delayMillis)
          } catch {
            case ie: InterruptedException => failInterrupted(ie)
          }
          attempt += 1
          delayMillis *= 2
      }
    }
  }
}

object FileService {
  def main(args: Array[String]): Unit = {
    // Set the configuration file's path
    val configFilePath = Path
      .of(sys.env.getOrElse("TEXERA_HOME", "."))
      .resolve("file-service")
      .resolve("src")
      .resolve("main")
      .resolve("resources")
      .resolve("file-service-web-config.yaml")
      .toAbsolutePath
      .toString

    // Start the Dropwizard application
    new FileService().run("server", configFilePath)
  }
}
