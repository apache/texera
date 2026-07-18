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

package org.apache.texera.auth

import com.fasterxml.jackson.module.scala.DefaultScalaModule
import io.dropwizard.configuration.{EnvironmentVariableSubstitutor, SubstitutingSourceProvider}
import io.dropwizard.core.{Application, Configuration}
import io.dropwizard.core.setup.Bootstrap
import org.apache.texera.common.config.StorageConfig
import org.apache.texera.dao.SqlServer

import java.nio.file.Path

/** Shared Dropwizard service bootstrap steps, identical across every Texera
  * service. Kept here so the services don't drift apart.
  */
object ServiceBootstrap {

  /** Enable `${ENV_VAR}` substitution in the YAML config and register the Scala
    * module on Dropwizard's default object mapper.
    */
  def configure[T <: Configuration](bootstrap: Bootstrap[T]): Unit = {
    // enable environment variable substitution in YAML config
    bootstrap.setConfigurationSourceProvider(
      new SubstitutingSourceProvider(
        bootstrap.getConfigurationSourceProvider,
        new EnvironmentVariableSubstitutor(false)
      )
    )
    // register Scala module to Dropwizard default object mapper
    bootstrap.getObjectMapper.registerModule(DefaultScalaModule)
  }

  /** Open the shared SQL connection pool using the storage configuration. */
  def initDatabase(): Unit =
    SqlServer.initConnection(
      StorageConfig.jdbcUrl,
      StorageConfig.jdbcUsername,
      StorageConfig.jdbcPassword
    )

  /** Resolve `$TEXERA_HOME/<serviceDir>/src/main/resources/<configFileName>` to
    * an absolute path string, the convention every service `main` uses.
    */
  def configFilePath(serviceDir: String, configFileName: String): String =
    Path
      .of(sys.env.getOrElse("TEXERA_HOME", "."))
      .resolve(serviceDir)
      .resolve("src")
      .resolve("main")
      .resolve("resources")
      .resolve(configFileName)
      .toAbsolutePath
      .toString

  /** Launch the Dropwizard `server` command against the conventional config
    * path, the shared shape of every service `main`.
    */
  def start(
      app: Application[_ <: Configuration],
      serviceDir: String,
      configFileName: String
  ): Unit =
    app.run("server", configFilePath(serviceDir, configFileName))
}
