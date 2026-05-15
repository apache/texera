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

package org.apache.texera.workflow.macroOp

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.typesafe.scalalogging.LazyLogging
import org.apache.texera.amber.operator.macroOp.MacroBody
import org.apache.texera.dao.SqlServer
import org.apache.texera.dao.jooq.generated.Tables.WORKFLOW
import org.apache.texera.dao.jooq.generated.enums.WorkflowKindEnum

import scala.util.control.NonFatal

/**
  * jOOQ + Jackson-backed [[MacroRegistry]] for the amber execution-time
  * compiler. Reads `workflow.content` as a JSON-serialized [[MacroBody]] —
  * same shape produced by `MacroResource.create`.
  *
  * v1 ignores the `version` argument and always reads the current row.
  * Reconstructing a specific `vid` from `workflow_version` patches is deferred
  * to Phase 2.
  *
  * Duplicates the compiling-service `DbMacroRegistry`; both share the same
  * `texera_db` schema so the body bytes round-trip identically across paths.
  */
class DbMacroRegistry extends MacroRegistry with LazyLogging {

  private val mapper = new ObjectMapper().registerModule(DefaultScalaModule)

  override def fetch(macroId: String, version: Int): Option[MacroBody] = {
    val widOpt =
      try Some(Integer.parseInt(macroId))
      catch { case _: NumberFormatException => None }

    widOpt.flatMap { wid =>
      try {
        val record = SqlServer
          .getInstance()
          .createDSLContext()
          .select(WORKFLOW.CONTENT, WORKFLOW.KIND)
          .from(WORKFLOW)
          .where(WORKFLOW.WID.eq(wid))
          .fetchOne()
        if (record == null || record.value2() != WorkflowKindEnum.MACRO) {
          None
        } else {
          Option(record.value1())
            .filter(_.nonEmpty)
            .map(mapper.readValue(_, classOf[MacroBody]))
        }
      } catch {
        case NonFatal(e) =>
          logger.error(
            s"DbMacroRegistry: failed to load macro macroId=$macroId version=$version",
            e
          )
          None
      }
    }
  }
}
