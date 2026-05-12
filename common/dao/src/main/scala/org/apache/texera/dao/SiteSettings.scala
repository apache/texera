/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.texera.dao

import org.jooq.impl.DSL

import scala.util.Try

/**
  * Read-side accessor for the `site_settings` key/value table that admin pages
  * write through. Centralises the "look up by key, parse, fall back on any
  * failure" pattern that previously lived inline in ConfigResource,
  * CSVScanSourceOpExec, and DatasetResource.
  *
  * Failures swallowed by the outer Try include: SqlServer not initialised
  * (e.g. on workers in distributed mode), no row for the key, and value that
  * can't be parsed. In all of these cases the caller's default takes over.
  */
object SiteSettings {

  def getInt(key: String, default: => Int): Int =
    readAndParse(key, default)(_.toInt)

  def getLong(key: String, default: => Long): Long =
    readAndParse(key, default)(_.toLong)

  private[dao] def parseOrDefault[T](raw: Option[String], default: T)(parse: String => T): T =
    raw.flatMap(s => Try(parse(s.trim)).toOption).getOrElse(default)

  private def readAndParse[T](key: String, default: => T)(parse: String => T): T =
    Try {
      val raw = SqlServer
        .getInstance()
        .createDSLContext()
        .select(DSL.field("value", classOf[String]))
        .from(DSL.table(DSL.name("texera_db", "site_settings")))
        .where(DSL.field("key", classOf[String]).eq(key))
        .fetchOneInto(classOf[String])
      parseOrDefault(Option(raw), default)(parse)
    }.getOrElse(default)
}
