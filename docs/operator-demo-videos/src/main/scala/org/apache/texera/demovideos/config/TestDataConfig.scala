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

package org.apache.texera.demovideos.config

case class UiConfig(
    recordWidth: Int,
    recordHeight: Int,
    slowMo: Int,
    resultPanelHoldMs: Int,
    propertyPanelResizeHeight: Double,
    operatorPosX: Double,
    operatorPosY: Double
)

object TestDataConfig {
  val baseUrl = "http://localhost:4200"

  val uiConfig = UiConfig(
    // 1080p: the canvas, the left operator panel, and the property panel all have to be
    // usable at once. Below this the operator list needs scrolling before an item can be
    // dragged, and the recording is cramped to watch.
    recordWidth = 1920,
    recordHeight = 1080,
    slowMo = 400,
    resultPanelHoldMs = 5000,
    propertyPanelResizeHeight = 300.0,
    operatorPosX = 0.33,
    operatorPosY = 0.4
  )

  val videoOutputDir = "docs/operator-demo-videos/generated"
}
