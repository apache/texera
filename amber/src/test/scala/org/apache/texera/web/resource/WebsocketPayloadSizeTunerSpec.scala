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

package org.apache.texera.web.resource

import org.scalamock.scalatest.MockFactory
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import javax.servlet.{ServletContext, ServletContextEvent}
import javax.websocket.server.ServerContainer

class WebsocketPayloadSizeTunerSpec extends AnyFlatSpec with Matchers with MockFactory {

  private def eventWith(container: ServerContainer): ServletContextEvent = {
    val servletContext = mock[ServletContext]
    (servletContext
      .getAttribute(_: String))
      .expects(classOf[ServerContainer].getName)
      .returning(container)
      .once()
    new ServletContextEvent(servletContext)
  }

  "contextInitialized" should "look up the server container by its class name and tune both limits" in {
    val maxKiB = 64
    val container = mock[ServerContainer]
    (container.setDefaultMaxTextMessageBufferSize(_: Int)).expects(maxKiB * 1024).once()
    (container.setDefaultMaxBinaryMessageBufferSize(_: Int)).expects(maxKiB * 1024).once()

    new WebsocketPayloadSizeTuner(maxKB).contextInitialized(eventWith(container))
  }

  it should "convert a one-KiB payload limit to exactly 1024 bytes" in {
    val container = mock[ServerContainer]
    (container.setDefaultMaxTextMessageBufferSize(_: Int)).expects(1024).once()
    (container.setDefaultMaxBinaryMessageBufferSize(_: Int)).expects(1024).once()

    new WebsocketPayloadSizeTuner(1).contextInitialized(eventWith(container))
  }
}
