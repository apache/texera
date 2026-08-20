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

package org.apache.texera.service.resource

import jakarta.ws.rs.BadRequestException
import org.apache.texera.dao.SqlStates
import org.jooq.{DSLContext, Record}
import org.jooq.exception.DataAccessException

/**
  * Naming rules shared by every user-owned resource: what a name may contain, and that a name
  * is unique among one owner's resources of that type.
  */
object ResourceNaming {

  private val NAME_MAX_LENGTH = 128
  private val NAME_PATTERN = "^[A-Za-z0-9_-]+$".r

  /**
    * Rules:
    * - Must be 1 to 128 characters long.
    * - Only letters, numbers, underscores, and hyphens are allowed.
    *
    * @throws jakarta.ws.rs.BadRequestException if the name is invalid.
    */
  def validateName(label: String, name: String): Unit = {
    if (name == null || !NAME_PATTERN.matches(name)) {
      throw new BadRequestException(
        s"Invalid $label name: only letters, numbers, underscores, and hyphens are allowed."
      )
    }
    if (name.length > NAME_MAX_LENGTH) {
      throw new BadRequestException(
        s"Invalid $label name: name must be at most $NAME_MAX_LENGTH characters long."
      )
    }
  }

  /**
    * Rejects a name the owner already uses for another resource of the same type.
    *
    * @param excludingId the resource being renamed, so it does not conflict with itself
    */
  def requireNameAvailable[R <: Record, A <: Record](
      ctx: DSLContext,
      resource: ResourceTables[R, A],
      ownerUid: Integer,
      name: String,
      excludingId: Option[Integer] = None
  ): Unit = {
    val taken = ctx.fetchExists(
      excludingId.foldLeft(
        ctx
          .selectFrom(resource.table)
          .where(resource.ownerUidField.eq(ownerUid))
          .and(resource.nameField.eq(name))
      )((query, id) => query.and(resource.idField.notEqual(id)))
    )
    if (taken) {
      throw duplicateName(resource.label)
    }
  }

  /**
    * Runs a write and translates an (owner_uid, name) unique-constraint violation into the same
    * BadRequestException the pre-check throws, so requests losing a concurrent race get a 400
    * instead of a 500.
    */
  def failOnDuplicateName[T](label: String)(op: => T): T = {
    try op
    catch {
      case e: DataAccessException =>
        if (e.sqlState() == SqlStates.UNIQUE_VIOLATION) {
          throw duplicateName(label)
        }
        throw e
    }
  }

  private def duplicateName(label: String): BadRequestException =
    new BadRequestException(s"${label.capitalize} with the same name already exists")
}
