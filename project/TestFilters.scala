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
 * Selects a module's tagged tests for the fast-unit job or the integration job:
 * skip-integration excludes them, integration-only runs only them, unset runs
 * everything. Shared because the mapping is identical in every module, while the
 * env var and the tag are not — the tag annotation has to live somewhere the
 * module's own Test config can see.
 */
object TestFilters {

  /** @param integrationOnlyExtra further ScalaTest args for the integration side,
   *                              e.g. "-P4" to bound its pool when a suite forks
   *                              a process per test.
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
