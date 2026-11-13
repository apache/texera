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

package org.apache.texera.amber.testsupport.setop

import org.apache.texera.amber.core.tuple.{AttributeType, Schema, Tuple, TupleLike}

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.util.concurrent.TimeUnit
import scala.io.Source

/** The two inputs the set operators' parity tests share, fed to the native
  * executor as tuples and to the export as nullable Arrow-style columns.
  *
  * Each side holds a finite value and a null the other side also holds, as
  * controls, next to a NaN and a zero of each sign. The left holds its NaN
  * twice, since the engine keeps two NaN rows apart as well.
  */
object SetRows {
  private val schema = Schema().add("k", AttributeType.STRING).add("v", AttributeType.DOUBLE)

  private val leftValues: Seq[java.lang.Double] = Seq(1.0, Double.NaN, 0.0, null, 2.0, Double.NaN)
  private val rightValues: Seq[java.lang.Double] = Seq(1.0, Double.NaN, -0.0, null, 3.0)

  val left: Seq[Tuple] = leftValues.map(v => Tuple(schema, Array[Any]("a", v)))
  val right: Seq[Tuple] = rightValues.map(v => Tuple(schema, Array[Any]("a", v)))

  /** The native output, one sorted line per row, written as the driver writes it. */
  def rendered(rows: Iterator[TupleLike]): Seq[String] =
    rows.map(_.getFields.map(render).mkString(" ")).toSeq.sorted

  /** Runs `exported` over the same rows and returns what it answered, as above. */
  def runExport(python: String, exported: String, prefix: String): Seq[String] = {
    val driver =
      s"""import pandas as pd, numpy as np
         |
         |def col(vals):
         |    mask = np.array([v is None for v in vals])
         |    return pd.arrays.FloatingArray(np.array([0.0 if v is None else v for v in vals]), mask)
         |
         |in1df = pd.DataFrame({"k": ["a"] * ${leftValues.size}, "v": col(${pyList(leftValues)})})
         |in2df = pd.DataFrame({"k": ["a"] * ${rightValues.size}, "v": col(${pyList(rightValues)})})
         |$exported
         |def render(v):
         |    return "None" if v is pd.NA else v if isinstance(v, str) else repr(float(v))
         |for row in sorted(" ".join(render(v) for v in r) for r in out1df.itertuples(index=False)):
         |    print(row)
         |""".stripMargin
    val script = Files.createTempFile(prefix, ".py")
    script.toFile.deleteOnExit()
    Files.write(script, driver.getBytes(StandardCharsets.UTF_8))
    val process = new ProcessBuilder(python, script.toString).redirectErrorStream(true).start()
    val out = Source.fromInputStream(process.getInputStream, "UTF-8").mkString
    process.waitFor(120, TimeUnit.SECONDS)
    if (process.exitValue() != 0) {
      throw new AssertionError(s"python said:\n$out\nscript:\n$driver")
    }
    out.linesIterator.toSeq
  }

  private def render(value: Any): String =
    value match {
      case null                           => "None"
      case d: java.lang.Double if d.isNaN => "nan"
      case other                          => other.toString
    }

  private def pyList(values: Seq[java.lang.Double]): String =
    values
      .map(v => if (v == null) "None" else if (v.isNaN) "np.nan" else v.toString)
      .mkString("[", ", ", "]")
}
