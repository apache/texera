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

package org.apache.texera.amber.operator.source.scan

import scala.collection.mutable.ArrayBuffer

/**
  * Accumulates the warnings for rows a scan source skipped because they did not parse
  * into the inferred schema. Shared by all scan variants (CSV, JSONL, ParallelCSV,
  * csvOld) so they surface skipped rows identically.
  *
  * Only the first `maxDetailed` rows are listed individually; the total is always
  * tracked, and when it exceeds the cap a single summary line reports how many more
  * were skipped. This keeps a pathological file (e.g. hundreds of thousands of bad
  * rows) from exhausting memory or flooding the console.
  *
  * @param maxDetailed maximum number of rows reported individually
  */
class SkippedRowReporter(maxDetailed: Int = SkippedRowReporter.DefaultMaxDetailed) {

  private val detailed = ArrayBuffer.empty[String]
  private var total = 0

  /** Records one skipped row. `warning` is evaluated lazily so building the (relatively
    * expensive) per-row message is skipped once the detail cap is reached.
    */
  def record(warning: => String): Unit = {
    total += 1
    if (detailed.size < maxDetailed) detailed += warning
  }

  /** Total number of rows skipped so far. */
  def count: Int = total

  /**
    * The warnings to surface: one line per detailed row, plus a trailing summary line
    * when more rows were skipped than the cap allows. Empty when nothing was skipped.
    */
  def warnings: Seq[String] = {
    if (total == 0) Seq.empty
    else if (total > detailed.size)
      detailed.toSeq :+ ScanRowParseError.moreSkipped(total - detailed.size, total)
    else detailed.toSeq
  }
}

object SkippedRowReporter {
  val DefaultMaxDetailed: Int = 100
}
