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

package org.apache.texera.amber.translator.verify

import org.apache.texera.amber.operator.source.SourceOperatorDescriptor

import java.nio.file.Files

/**
  * Reads every operator's generated code and reports one that writes to the
  * frame it was handed.
  *
  * The translator names a variable per output PORT, not per reader, so two
  * operators drawn from one upstream are handed the same name. An operator that
  * drops rows into that name, or assigns a column through it, changes what the
  * other branch goes on to read: a bar chart that drops its own nulls would
  * leave a pie chart beside it drawing a table it never received.
  *
  * Both spellings of the mistake matter, and one of them does not look like a
  * mistake. `inplace=True` mutates the frame outright. `in1df = in1df.dropna()`
  * reads like a local rebinding, but the operator bodies are concatenated at
  * module scope, so the name it rebinds is the shared one.
  *
  * A single-branch workflow never notices either, which is why this reads the
  * code rather than waiting for a run to disagree.
  */
object StandaloneInputCheck {

  private val Assignment = """(?m)^\s*in\d+df\s*\[[^\]]*\]\s*=""".r
  private val InPlace = """in\d+df[^\n]*inplace\s*=\s*True""".r
  private val Rebind = """(?m)^\s*in\d+df\s*=""".r

  /** Every operator that writes to its input, empty when the suite is clean. */
  def run(): Seq[String] = {
    val operators = OperatorBehaviorSpec
      .discoverStandaloneOperators()
      .filterNot(classOf[SourceOperatorDescriptor].isAssignableFrom)
    val dir = Files.createTempDirectory("standalone-input-")
    dir.toFile.deleteOnExit()

    operators.flatMap { op =>
      StandaloneEscapingCheck.codeFor(op, dir) match {
        // An operator whose config cannot be built is reported by the escaping
        // check, which builds the same ones; repeating it here would say the
        // same thing twice.
        case Left(_) => Seq.empty
        case Right(variants) =>
          variants.flatMap {
            case (label, code) =>
              val how = Seq(
                Assignment.findFirstIn(code).map(_ => "assigns a column through it"),
                InPlace.findFirstIn(code).map(_ => "mutates it with inplace=True"),
                Rebind.findFirstIn(code).map(_ => "rebinds the name it was given")
              ).flatten
              if (how.isEmpty) None
              else Some(s"${op.getSimpleName}/$label: ${how.mkString(", ")}")
          }
      }
    }
  }
}
