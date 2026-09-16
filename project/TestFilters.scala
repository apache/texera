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
 * everything, and any other value fails the build rather than quietly running
 * everything in a job that selected a subset. Shared because the mapping is
 * identical in every module, while the env var and the tag are not — the tag
 * annotation has to live somewhere the module's own Test config can see.
 */
object TestFilters {

  /** @param envVar the variable the two CI jobs set to opposite values; it has to
   *                be the one the workflow already sets on the step that invokes
   *                this module's tests, or neither subset is selected.
   * @param tag     fully-qualified name of the tag annotation, as
   *                `classOf[...].getName` gives it at the test site — ScalaTest
   *                matches these by string, so a rename that misses one side
   *                silently stops filtering.
   */
  def integrationSplit(envVar: String, tag: String): Seq[TestOption] =
    sys.env.get(envVar) match {
      case Some("skip-integration") =>
        Seq(Tests.Argument(TestFrameworks.ScalaTest, "-l", tag))
      case Some("integration-only") =>
        Seq(Tests.Argument(TestFrameworks.ScalaTest, "-n", tag))
      case Some(other) => sys.error(s"$envVar=$other: use skip-integration or integration-only")
      case None        => Nil
    }
}
