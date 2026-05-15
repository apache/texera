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

import io.dropwizard.auth.Auth
import org.apache.texera.auth.SessionUser
import org.apache.texera.dao.SqlServer
import org.apache.texera.dao.jooq.generated.Tables.MACHINE
import org.apache.texera.dao.jooq.generated.tables.daos.MachineDao
import org.apache.texera.dao.jooq.generated.tables.pojos.Machine

import javax.annotation.security.RolesAllowed
import javax.ws.rs._
import javax.ws.rs.core.MediaType

object MachineResource {
  case class MachineRequest(name: String, url: String, token: Option[String])
}

@Path("/machines")
@Produces(Array(MediaType.APPLICATION_JSON))
@Consumes(Array(MediaType.APPLICATION_JSON))
@RolesAllowed(Array("REGULAR", "ADMIN"))
class MachineResource {
  import MachineResource._

  private def dao: MachineDao =
    new MachineDao(SqlServer.getInstance().createDSLContext().configuration())

  @GET
  def list(@Auth user: SessionUser): java.util.List[Machine] = {
    SqlServer
      .getInstance()
      .createDSLContext()
      .selectFrom(MACHINE)
      .where(MACHINE.UID.eq(user.getUid))
      .fetchInto(classOf[Machine])
  }

  @POST
  def create(@Auth user: SessionUser, req: MachineRequest): Machine = {
    require(req.name != null && req.name.trim.nonEmpty, "name required")
    require(req.url != null && req.url.trim.nonEmpty, "url required")
    val m = new Machine()
    m.setUid(user.getUid)
    m.setName(req.name.trim)
    m.setUrl(req.url.trim)
    m.setToken(req.token.map(_.trim).filter(_.nonEmpty).orNull)
    dao.insert(m)
    m
  }

  @GET
  @Path("/{mid}")
  def get(@Auth user: SessionUser, @PathParam("mid") mid: Integer): Machine = {
    val m = dao.fetchOneByMid(mid)
    if (m == null || m.getUid != user.getUid) throw new NotFoundException()
    m
  }

  @PUT
  @Path("/{mid}")
  def update(
      @Auth user: SessionUser,
      @PathParam("mid") mid: Integer,
      req: MachineRequest
  ): Machine = {
    val m = dao.fetchOneByMid(mid)
    if (m == null || m.getUid != user.getUid) throw new NotFoundException()
    if (req.name != null && req.name.trim.nonEmpty) m.setName(req.name.trim)
    if (req.url != null && req.url.trim.nonEmpty) m.setUrl(req.url.trim)
    m.setToken(req.token.map(_.trim).filter(_.nonEmpty).orNull)
    dao.update(m)
    m
  }

  @DELETE
  @Path("/{mid}")
  def delete(@Auth user: SessionUser, @PathParam("mid") mid: Integer): Unit = {
    val m = dao.fetchOneByMid(mid)
    if (m == null || m.getUid != user.getUid) throw new NotFoundException()
    dao.delete(m)
  }
}
