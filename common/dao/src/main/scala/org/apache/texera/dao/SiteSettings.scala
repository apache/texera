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

import org.apache.texera.dao.jooq.generated.Tables.SITE_SETTINGS
import org.jooq.DSLContext
import org.jooq.impl.DSL

import scala.util.Try

/**
  * Accessor for the `site_settings` key/value table that admin pages write
  * through. Centralises the "look up by key, parse, fall back on any failure"
  * read pattern (previously inline in ConfigResource, CSVScanSourceOpExec, and
  * DatasetResource) and the write shape used by the admin API and the
  * config-service startup seeder, so the column set / audit stamping live in
  * one place.
  *
  * Failures swallowed by the outer Try on the read path include: SqlServer not
  * initialised (e.g. on workers in distributed mode), no row for the key, and
  * value that can't be parsed. In all of these cases the caller's default takes
  * over. The write helpers take an explicit [[DSLContext]] so callers can run
  * them inside their own transaction and surface failures.
  */
object SiteSettings {

  def getInt(key: String, default: => Int): Int =
    readAndParse(key, default)(_.toInt)

  def getLong(key: String, default: => Long): Long =
    readAndParse(key, default)(_.toLong)

  /** Insert or overwrite the row for `key`, stamping who/when. */
  def upsert(ctx: DSLContext, key: String, value: String, updatedBy: String): Unit =
    ctx
      .insertInto(SITE_SETTINGS)
      .set(SITE_SETTINGS.KEY, key)
      .set(SITE_SETTINGS.VALUE, value)
      .set(SITE_SETTINGS.UPDATED_BY, updatedBy)
      .onConflict(SITE_SETTINGS.KEY)
      .doUpdate()
      .set(SITE_SETTINGS.VALUE, value)
      .set(SITE_SETTINGS.UPDATED_BY, updatedBy)
      .set(SITE_SETTINGS.UPDATED_AT, DSL.currentTimestamp())
      .execute()

  /** Seed the row for `key` only if it does not already exist (never overwrites
    * an admin-edited value). Used by the startup default seeder.
    */
  def insertIfAbsent(ctx: DSLContext, key: String, value: String, updatedBy: String): Unit =
    ctx
      .insertInto(SITE_SETTINGS)
      .set(SITE_SETTINGS.KEY, key)
      .set(SITE_SETTINGS.VALUE, value)
      .set(SITE_SETTINGS.UPDATED_BY, updatedBy)
      .set(SITE_SETTINGS.UPDATED_AT, DSL.currentTimestamp())
      .onDuplicateKeyIgnore()
      .execute()

  private[dao] def parseOrDefault[T](raw: Option[String], default: T)(parse: String => T): T =
    raw.flatMap(s => Try(parse(s.trim)).toOption).getOrElse(default)

  private def readAndParse[T](key: String, default: => T)(parse: String => T): T =
    Try {
      val raw = SqlServer
        .getInstance()
        .createDSLContext()
        .select(SITE_SETTINGS.VALUE)
        .from(SITE_SETTINGS)
        .where(SITE_SETTINGS.KEY.eq(key))
        .fetchOneInto(classOf[String])
      parseOrDefault(Option(raw), default)(parse)
    }.getOrElse(default)
}
