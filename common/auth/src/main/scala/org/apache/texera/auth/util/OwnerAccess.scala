// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.texera.auth.util

import jakarta.ws.rs.ForbiddenException
import org.apache.texera.dao.jooq.generated.Tables.USER
import org.apache.texera.dao.jooq.generated.enums.PrivilegeEnum
import org.jooq.DSLContext

/** The rules every shared resource applies to its owner and to the users it is shared with. */
object OwnerAccess {

  /** The owner always has WRITE; anyone else has what they were granted, or NONE.
    * `granted` is by-name, so a lookup passed in is skipped for the owner.
    */
  def privilege(isOwner: Boolean, granted: => PrivilegeEnum): PrivilegeEnum =
    if (isOwner) PrivilegeEnum.WRITE else Option(granted).getOrElse(PrivilegeEnum.NONE)

  val ownerChangeRefused = "You cannot modify the owner's permissions!"

  /** Refuses any change to the owner's own access. */
  def requireNotOwner(isOwner: Boolean): Unit =
    if (isOwner) throw new ForbiddenException(ownerChangeRefused)

  /** The uid of the registered account with this email, or None when the email is unknown or
    * belongs to a placeholder; neither can be granted access.
    */
  def registeredUid(ctx: DSLContext, email: String): Option[Integer] =
    Option(
      ctx
        .select(USER.UID)
        .from(USER)
        .where(USER.EMAIL.eq(email).and(USER.IS_PLACEHOLDER.isFalse))
        .fetchOne(USER.UID)
    )
}
