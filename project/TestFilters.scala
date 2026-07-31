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

import sbt._

/**
 * Shared wiring for the "fast unit job / integration job" test split.
 *
 * A module that owns specs too expensive or too dependency-hungry for the fast
 * unit job tags them, then asks for this split so the two CI jobs can select
 * disjoint subsets by tag. Each module supplies its own env var and tag name,
 * because the tag annotation has to live in a module its own Test config can
 * see; the selection logic itself is identical everywhere and lives here.
 *
 *   skip-integration : exclude tagged specs (the fast unit job)
 *   integration-only : run only tagged specs (the job with the extra provisioning)
 *   unset            : run everything, which is the normal local behavior
 */
object TestFilters {

  /**
   * @param envVar               name of the env var the CI jobs set
   * @param tag                  fully-qualified tag name, as ScalaTest's -n/-l expect
   * @param integrationOnlyExtra extra ScalaTest args for the integration side only
   *                             (e.g. "-P4" to bound its parallel pool)
   */
  def integrationSplit(
      envVar: String,
      tag: String,
      integrationOnlyExtra: Seq[String] = Seq.empty
  ): Seq[TestOption] =
    sys.env.get(envVar) match {
      case Some("skip-integration") =>
        Seq(Tests.Argument(TestFrameworks.ScalaTest, "-l", tag))
      case Some("integration-only") =>
        Seq(Tests.Argument(TestFrameworks.ScalaTest, Seq("-n", tag) ++ integrationOnlyExtra: _*))
      case _ => Nil
    }
}
