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

package org.apache.texera.common.config

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
  * Spec for [[PekkoConfig]]. Reading keys off the returned config forces resolution of cluster.conf
  * (merged with the Typesafe default application config), so a renamed or mistyped key surfaces here
  * as a ConfigException. The log level carries a `${?ENV}` override, so its assertion is guarded.
  */
class PekkoConfigSpec extends AnyFlatSpec with Matchers {

  "PekkoConfig.pekkoConfig" should "load the actor/serialization settings from cluster.conf" in {
    val config = PekkoConfig.pekkoConfig
    config.getString("pekko.actor.provider") shouldBe "cluster"
    config.getBoolean("pekko.actor.allow-java-serialization") shouldBe false
    config.getBoolean("pekko.actor.enable-additional-serialization-bindings") shouldBe true
    config.getString(
      "pekko.actor.serializers.kryo"
    ) shouldBe "io.altoo.serialization.kryo.pekko.PekkoKryoSerializer"
    config.getStringList("pekko.loggers").get(0) shouldBe "org.apache.pekko.event.slf4j.Slf4jLogger"
    config.getString(
      "pekko.logging-filter"
    ) shouldBe "org.apache.pekko.event.slf4j.Slf4jLoggingFilter"
  }

  it should "expose the remote/artery transport settings" in {
    val config = PekkoConfig.pekkoConfig
    config.getString("pekko.remote.artery.transport") shouldBe "tcp"
    config.getString("pekko.remote.artery.canonical.hostname") shouldBe "0.0.0.0"
    config.getInt("pekko.remote.artery.canonical.port") shouldBe 0
    config.getBytes("pekko.remote.artery.advanced.maximum-frame-size") shouldBe 31457280L
    config.getBytes("pekko.remote.artery.advanced.maximum-large-frame-size") shouldBe 125829120L
  }

  it should "expose the cluster and failure-detector settings" in {
    val config = PekkoConfig.pekkoConfig
    config.getStringList("pekko.cluster.seed-nodes").size shouldBe 0
    config.getString(
      "pekko.cluster.downing-provider-class"
    ) shouldBe "org.apache.pekko.cluster.sbr.SplitBrainResolverProvider"
    config.getString("pekko.cluster.gossip-interval") shouldBe "10s"
    config.getString("pekko.cluster.failure-detector.acceptable-heartbeat-pause") shouldBe "50s"
    config.getString(
      "pekko-kryo-serialization.kryo-initializer"
    ) shouldBe "org.apache.texera.amber.engine.common.AmberKryoInitializer"
  }

  it should "resolve the log levels" in {
    val config = PekkoConfig.pekkoConfig
    // ${?TEXERA_SERVICE_LOG_LEVEL} can be satisfied by an OS env var or a JVM system property
    if (
      !sys.env
        .contains("TEXERA_SERVICE_LOG_LEVEL") && !sys.props.contains("TEXERA_SERVICE_LOG_LEVEL")
    ) {
      config.getString("pekko.loglevel") shouldBe "INFO"
    }
    config.getString("pekko.stdout-loglevel") shouldBe "INFO"
  }
}
