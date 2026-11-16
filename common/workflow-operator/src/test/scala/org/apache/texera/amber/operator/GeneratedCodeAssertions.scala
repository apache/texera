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

package org.apache.texera.amber.operator

import java.nio.charset.StandardCharsets
import java.util.Base64

private[operator] object GeneratedCodeAssertions {
  def b64(s: String): String =
    Base64.getEncoder.encodeToString(s.getBytes(StandardCharsets.UTF_8))

  def carries(output: String, name: String): Boolean =
    output.contains(name) || output.contains(b64(name))

  def carriesEncoded(output: String, name: String): Boolean =
    output.contains(b64(name))

  def fieldPart(msg: String): String =
    msg.toLowerCase.replace("cannot be empty", "")
}

private[operator] trait GeneratedCodeAssertionChecks
    extends org.scalatest.matchers.should.Matchers {
  import org.scalactic.source.Position
  import scala.reflect.ClassTag

  protected final def b64(s: String): String = GeneratedCodeAssertions.b64(s)
  protected def carries(output: String, name: String): Boolean =
    GeneratedCodeAssertions.carries(output, name)
  protected final def fieldPart(message: String): String =
    GeneratedCodeAssertions.fieldPart(message)

  protected final def expectError[T <: Throwable: ClassTag](phrase: String)(body: => Any)(implicit
      pos: Position
  ): T = {
    val error = intercept[T](body)
    error.getMessage should not be null
    error.getMessage should include(phrase)
    error
  }
}

// Literal column names can also match fixed Python identifiers, tasks or labels and hide a
// missing runtime splice. Check EncodableString columns in their Base64 form only.
private[operator] trait EncodedGeneratedCodeAssertionChecks extends GeneratedCodeAssertionChecks {
  override protected def carries(output: String, name: String): Boolean =
    GeneratedCodeAssertions.carriesEncoded(output, name)
}
