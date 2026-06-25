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
package org.apache.texera.web.resource.auth

import org.apache.texera.auth.SessionUser
import org.apache.texera.dao.jooq.generated.enums.UserRoleEnum
import org.apache.texera.dao.jooq.generated.tables.pojos.User
import org.scalatest.flatspec.AnyFlatSpec

class GoogleDriveAuthResourceSpec extends AnyFlatSpec {

  private def newSessionUser(): SessionUser = {
    val user = new User
    user.setUid(Integer.valueOf(1))
    user.setName("test")
    user.setRole(UserRoleEnum.REGULAR)
    user.setEmail("test@example.com")
    new SessionUser(user)
  }

  it should "return error HTML when code is missing" in {
    val resource = new GoogleDriveAuthResource()
    val response = resource.getCallback(code = "", state = "some-state")
    val body = response.getEntity.toString
    assert(body.contains("gdrive-error"))
    assert(body.contains("invalid request"))
  }

  it should "return error HTML when state is missing" in {
    val resource = new GoogleDriveAuthResource()
    val response = resource.getCallback(code = "some-code", state = "")
    val body = response.getEntity.toString
    assert(body.contains("gdrive-error"))
    assert(body.contains("invalid request"))
  }

  it should "return error HTML when state is not a pending OAuth request" in {
    val resource = new GoogleDriveAuthResource()
    val response = resource.getCallback(code = "some-code", state = "unknown-state-token")
    val body = response.getEntity.toString
    assert(body.contains("gdrive-error"))
    assert(body.contains("expired"))
  }

  it should "return a Google OAuth URL containing the drive.file scope" in {
    val resource = new GoogleDriveAuthResource()
    val response = resource.getOAuth(newSessionUser())
    val url = response.getEntity.toString
    assert(url.contains("accounts.google.com"))
    assert(url.contains("drive.file"))
  }

  it should "return a Google OAuth URL containing a state parameter" in {
    val resource = new GoogleDriveAuthResource()
    val response = resource.getOAuth(newSessionUser())
    val url = response.getEntity.toString
    assert(url.contains("state="))
  }

  it should "return a Google OAuth URL with a localhost redirect URI when no domain is configured" in {
    val resource = new GoogleDriveAuthResource()
    val response = resource.getOAuth(newSessionUser())
    val url = response.getEntity.toString
    assert(url.contains("localhost"))
    assert(url.contains("auth%2Fgoogle%2Fdrive%2Fcallback"))
  }

  it should "reject a state token that has already been used" in {
    val resource = new GoogleDriveAuthResource()
    val connectResponse = resource.getOAuth(newSessionUser())
    val oauthUrl = connectResponse.getEntity.toString
    val state = oauthUrl.split("state=").last.split("&").head

    // First callback removes the state token (then fails at Google token exchange with empty credentials)
    resource.getCallback(code = "any-code", state = state)

    // Second callback with same state should see the token is gone
    val response = resource.getCallback(code = "any-code", state = state)
    val body = response.getEntity.toString
    assert(body.contains("gdrive-error"))
    assert(body.contains("expired"))
  }

}
