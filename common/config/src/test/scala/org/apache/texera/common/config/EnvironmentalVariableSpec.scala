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

package org.apache.texera.common.config

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
  * Spec for [[EnvironmentalVariable]], the registry of environment-variable *names* used across
  * the *.conf files. The constants are enumerated reflectively (every no-arg String accessor on the
  * object) so a newly added `ENV_*` constant is automatically covered by the non-empty and
  * uniqueness checks — the list never drifts out of sync with the object.
  */
class EnvironmentalVariableSpec extends AnyFlatSpec with Matchers {

  // Each `val ENV_* : String` compiles to a public no-arg getter named `ENV_*` returning String.
  // Enumerating those getters reflects the object's constants without a hand-maintained list; the
  // `ENV_` name prefix also excludes inherited members such as `toString`.
  private val envVarNames: Seq[String] =
    EnvironmentalVariable.getClass.getMethods.toSeq
      .filter(m =>
        m.getName.startsWith("ENV_") &&
          m.getParameterCount == 0 &&
          m.getReturnType == classOf[String]
      )
      .map(_.invoke(EnvironmentalVariable).asInstanceOf[String])

  // A well-formed env-var name: an uppercase letter followed by uppercase letters, digits, or
  // underscores. Rejects empty, all-whitespace, and padded names (e.g. "STORAGE_JDBC_URL ") that
  // would silently miss env lookups — a stricter check than `trim.nonEmpty`.
  private val nameShape = "[A-Z][A-Z0-9_]*".r

  "EnvironmentalVariable" should "expose a non-trivial set of env-var name constants" in {
    envVarNames.size should be > 20
  }

  it should "expose well-formed, unique env-var names" in {
    envVarNames.foreach(name =>
      withClue(s"malformed env-var name [$name]: ")(name should fullyMatch regex nameShape)
    )
    envVarNames.distinct.size shouldBe envVarNames.size
  }

  it should "use the documented names for a representative sample" in {
    EnvironmentalVariable.ENV_JAVA_OPTS shouldBe "JAVA_OPTS"
    EnvironmentalVariable.ENV_AUTH_JWT_SECRET shouldBe "AUTH_JWT_SECRET"
    EnvironmentalVariable.ENV_JDBC_URL shouldBe "STORAGE_JDBC_URL"
    EnvironmentalVariable.ENV_S3_ENDPOINT shouldBe "STORAGE_S3_ENDPOINT"
    EnvironmentalVariable.ENV_CACHE_ENABLED shouldBe "CACHE_ENABLED"
  }

  "EnvironmentalVariable.get" should "return None for an unset variable" in {
    val absent = "TEXERA_DEFINITELY_UNSET_" + System.nanoTime().toString
    EnvironmentalVariable.get(absent) shouldBe None
  }

  it should "return Some(value) for a variable that is set in the JVM environment" in {
    val env = System.getenv()
    if (env.isEmpty) cancel("no environment variables available to exercise the Some(value) case")
    val entry = env.entrySet().iterator().next()
    EnvironmentalVariable.get(entry.getKey) shouldBe Some(entry.getValue)
  }
}
