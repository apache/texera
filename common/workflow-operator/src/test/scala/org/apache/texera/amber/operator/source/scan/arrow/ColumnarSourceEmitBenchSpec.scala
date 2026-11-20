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

package org.apache.texera.amber.operator.source.scan.arrow

import org.apache.texera.amber.core.executor.{ColumnarResult, SourceOperatorExecutor}
import org.apache.texera.amber.core.tuple.{Schema, SchemaEnforceable, Tuple}
import org.apache.texera.amber.operator.filter.{
  ComparisonType,
  FilterPredicate,
  SpecializedFilterOpDesc,
  SpecializedFilterOpExec
}
import org.apache.texera.amber.operator.source.scan.parquet.{
  ParquetScanSourceOpDesc,
  ParquetScanSourceOpExec
}
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.scalatest.flatspec.AnyFlatSpec

import java.io.File

// Measures what wiring the columnar source buys on a real Arrow file: the row path builds a
// Tuple per row (each paying Tuple.inMemSize's deepSizeOf walk) then filters per tuple; the
// columnar path hands out the file's Arrow batches straight to the vectorized filter, no Tuple
// built. Same file, same single numeric predicate, same surviving-row count. Off unless
// -Dcolumnar.srcbench=true; the file is -Dcolumnar.srcbench.file=/abs/path/big.arrow.
class ColumnarSourceEmitBenchSpec extends AnyFlatSpec {

  private def enabled: Boolean = sys.props.get("columnar.srcbench").contains("true")
  private val arrowFile = sys.props.getOrElse("columnar.srcbench.file", "")
  private val filterCol = sys.props.getOrElse("columnar.srcbench.col", "c0")
  private val trials = sys.props.getOrElse("columnar.srcbench.trials", "5").toInt

  private def isParquet: Boolean = arrowFile.endsWith(".parquet")

  // The source executor for the file, Arrow or Parquet, both of which now have a native columnar
  // path (produceColumnarBatch).
  private def makeSource(): SourceOperatorExecutor = {
    val uri = new File(arrowFile).toURI.toString
    if (isParquet) {
      val d = new ParquetScanSourceOpDesc(); d.fileName = Some(uri)
      new ParquetScanSourceOpExec(objectMapper.writeValueAsString(d))
    } else {
      val d = new ArrowSourceOpDesc(); d.fileName = Some(uri)
      new ArrowSourceOpExec(objectMapper.writeValueAsString(d))
    }
  }

  // The port schema, for enforcing the Parquet row path's SeqTupleLike into a Tuple (the engine
  // does this per row). The Arrow source already yields Tuples, so it needs no schema (null).
  private def portSchema: Schema =
    if (!isParquet) null
    else {
      val d = new ParquetScanSourceOpDesc(); d.fileName = Some(new File(arrowFile).toURI.toString)
      d.inferSchema()
    }

  // c0 >= 0.0: a single vectorizable Float8 predicate, ~half the random doubles.
  private def filterDesc: String = {
    val d = new SpecializedFilterOpDesc()
    d.predicates =
      List(new FilterPredicate(filterCol, ComparisonType.GREATER_THAN_OR_EQUAL_TO, "0.0"))
    objectMapper.writeValueAsString(d)
  }

  private def median(xs: Seq[Double]): Double = xs.sorted.apply(xs.size / 2)

  private def timeRow(schema: Schema): (Double, Long) = {
    val exec = makeSource()
    exec.open()
    val filter = new SpecializedFilterOpExec(filterDesc)
    var survived = 0L
    val t0 = System.nanoTime()
    try {
      exec.produceTuple().foreach { tl =>
        // The engine enforces the port schema per row before the operator sees it; a source
        // already yielding a Tuple (Arrow) keeps it, a SeqTupleLike (Parquet) is enforced.
        val tuple = tl match {
          case t: Tuple              => t
          case se: SchemaEnforceable => se.enforceSchema(schema)
        }
        if (filter.processTuple(tuple, 0).hasNext) survived += 1
      }
    } finally { filter.close(); exec.close() }
    ((System.nanoTime() - t0) / 1e9, survived)
  }

  private def timeColumnar(): (Double, Long) = {
    val exec = makeSource()
    exec.open()
    val filter = new SpecializedFilterOpExec(filterDesc)
    var survived = 0L
    val t0 = System.nanoTime()
    try {
      exec.produceColumnarBatch().get.foreach { root =>
        try filter.processColumnarRoot(root, 0) match {
          case ColumnarResult.Emit(out) => survived += out.getRowCount; out.close()
          case _                        => survived += root.getRowCount
        } finally root.close()
      }
    } finally { filter.close(); exec.close() }
    ((System.nanoTime() - t0) / 1e9, survived)
  }

  "columnar source emit" should "run the row vs columnar source benchmark" in {
    assume(enabled && arrowFile.nonEmpty)
    val schema = portSchema
    timeRow(schema); timeColumnar() // warmup
    val rows = (1 to trials).map(_ => timeRow(schema))
    val cols = (1 to trials).map(_ => timeColumnar())
    val rowMed = median(rows.map(_._1))
    val colMed = median(cols.map(_._1))
    println(s"=== columnar source emit benchmark ($arrowFile, filter $filterCol >= 0.0) ===")
    println(s"row  survived=${rows.head._2} secs=${rows.map(_._1).sorted.mkString(", ")}")
    println(s"col  survived=${cols.head._2} secs=${cols.map(_._1).sorted.mkString(", ")}")
    println(f"row  median: $rowMed%.3fs")
    println(f"col  median: $colMed%.3fs")
    println(f"speedup row/col: ${rowMed / colMed}%.2fx")
    assert(rows.head._2 == cols.head._2, "both paths must select the same rows")
  }
}
