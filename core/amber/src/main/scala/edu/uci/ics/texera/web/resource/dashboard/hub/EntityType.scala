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

package edu.uci.ics.texera.web.resource.dashboard.hub

import com.fasterxml.jackson.annotation.{JsonCreator, JsonValue}

/**
  * Defines all supported entity types for Hub resources.
  * Enables JSON ↔ enum conversion with lowercase string representation.
  */
sealed trait EntityType {
  @JsonValue
  def value: String

  override def toString: String = value
}

object EntityType {
  case object Workflow extends EntityType { val value = "workflow" }
  case object Dataset extends EntityType { val value = "dataset" }

  private val values = Seq(Workflow, Dataset)

  @JsonCreator
  def fromString(s: String): EntityType =
    values
      .find(_.value.equalsIgnoreCase(s))
      .getOrElse(
        throw new IllegalArgumentException(s"Unsupported entityType '$s'")
      )
}
