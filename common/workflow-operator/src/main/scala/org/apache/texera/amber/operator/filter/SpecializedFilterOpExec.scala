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

package org.apache.texera.amber.operator.filter

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.{Float8Vector, IntVector, VectorSchemaRoot}
import org.apache.texera.amber.core.executor.{ColumnarOperatorExecutor, ColumnarResult}
import org.apache.texera.common.config.ApplicationConfig
import org.apache.texera.amber.core.tuple.{
  AttributeType,
  AttributeTypeUtils,
  ColumnarBatch,
  Tuple,
  TupleLike
}
import org.apache.texera.amber.core.workflow.PortIdentity
import org.apache.texera.amber.util.ArrowUtils
import org.apache.texera.amber.util.JSONUtils.objectMapper

import java.sql.Timestamp

class SpecializedFilterOpExec(descString: String)
    extends FilterOpExec
    with ColumnarOperatorExecutor {
  private val desc: SpecializedFilterOpDesc =
    objectMapper.readValue(descString, classOf[SpecializedFilterOpDesc])
  // Row path: OR over predicates, per tuple.
  setFilterFunc((tuple: Tuple) => desc.predicates.exists(_.evaluate(tuple)))

  private val vectorizedEnabled: Boolean = ApplicationConfig.enableVectorizedOperators

  // Per-predicate row test with per-batch invariants hoisted, resolved from the
  // batch schema on first use. null = a predicate shape is not vectorizable, so
  // the whole batch falls back to the row path.
  @transient private var compiled: Array[Tuple => Boolean] = _
  @transient private var compileAttempted: Boolean = false

  private def cmpOf(c: ComparisonType): Int => Boolean =
    c match {
      case ComparisonType.GREATER_THAN             => (r: Int) => r > 0
      case ComparisonType.GREATER_THAN_OR_EQUAL_TO => (r: Int) => r >= 0
      case ComparisonType.LESS_THAN                => (r: Int) => r < 0
      case ComparisonType.LESS_THAN_OR_EQUAL_TO    => (r: Int) => r <= 0
      case ComparisonType.EQUAL_TO                 => (r: Int) => r == 0
      case ComparisonType.NOT_EQUAL_TO             => (r: Int) => r != 0
      case _                                       => null
    }

  // Compile one predicate to a Tuple => Boolean matching FilterPredicate.evaluate,
  // hoisting the schema lookup and constant parse out of the per-row work.
  // Returns null if the shape isn't supported.
  private def compileOne(p: FilterPredicate, tpe: AttributeType): Tuple => Boolean = {
    val attr = p.attribute
    p.condition match {
      case ComparisonType.IS_NULL     => (t: Tuple) => t.getField[Any](attr) == null
      case ComparisonType.IS_NOT_NULL => (t: Tuple) => t.getField[Any](attr) != null
      case cond =>
        val cmp = cmpOf(cond)
        if (cmp == null) return null
        tpe match {
          case AttributeType.INTEGER | AttributeType.DOUBLE =>
            val c = try p.value.toDouble catch { case _: NumberFormatException => return null }
            (t: Tuple) => {
              val f = t.getField[Any](attr)
              if (f == null) false else cmp(java.lang.Double.compare(f.asInstanceOf[Number].doubleValue(), c))
            }
          case AttributeType.LONG =>
            val c = try java.lang.Long.valueOf(p.value.trim) catch { case _: NumberFormatException => return null }
            (t: Tuple) => {
              val f = t.getField[Any](attr)
              if (f == null) false else cmp(java.lang.Long.compare(f.asInstanceOf[Number].longValue(), c))
            }
          case AttributeType.TIMESTAMP =>
            val c = AttributeTypeUtils.parseTimestamp(p.value.trim).getTime
            (t: Tuple) => {
              val f = t.getField[Any](attr)
              if (f == null) false else cmp(java.lang.Long.compare(f.asInstanceOf[Timestamp].getTime, c))
            }
          case AttributeType.BOOLEAN =>
            val c = p.value.trim.toLowerCase
            (t: Tuple) => {
              val f = t.getField[Any](attr)
              if (f == null) false else cmp(f.toString.toLowerCase.compareTo(c))
            }
          case AttributeType.STRING | AttributeType.ANY =>
            // Mirror FilterPredicate.evaluateFilterString: numeric compare when
            // both field and value parse as double, else lexicographic.
            val valNum: java.lang.Double =
              try java.lang.Double.valueOf(p.value) catch { case _: NumberFormatException => null }
            (t: Tuple) => {
              val f = t.getField[Any](attr)
              if (f == null) false
              else {
                val s = f.toString
                if (valNum == null) cmp(s.compareTo(p.value))
                else {
                  val fd = try java.lang.Double.valueOf(s.trim) catch { case _: NumberFormatException => null }
                  if (fd == null) cmp(s.compareTo(p.value))
                  else cmp(java.lang.Double.compare(fd, valNum))
                }
              }
            }
          case _ => null
        }
    }
  }

  private def compileAll(sample: Tuple): Unit = {
    compileAttempted = true
    val schema = sample.getSchema
    val fns = desc.predicates.map { p =>
      val tpe = try schema.getAttribute(p.attribute).getType catch { case _: Throwable => null }
      if (tpe == null) null else compileOne(p, tpe)
    }
    if (fns.nonEmpty && fns.forall(_ != null)) compiled = fns.toArray
  }

  override def processBatchMultiPort(
      batch: Array[Tuple],
      port: Int
  ): Option[Iterator[(TupleLike, Option[PortIdentity])]] = {
    if (!vectorizedEnabled) return None
    if (batch.isEmpty) return Some(Iterator.empty)
    if (!compileAttempted) compileAll(batch(0))
    if (compiled == null) return None

    val cb = new ColumnarBatch(batch)
    val n = batch.length
    val mask = new Array[Boolean](n)

    if (compiled.length == 1 && desc.predicates.size == 1 && isNumericSimd(batch(0))) {
      // SIMD fast path: single numeric ordered/eq predicate over a primitive column.
      val p = desc.predicates.head
      val cmp = cmpOf(p.condition)
      val c = p.value.toDouble
      val (vals, isNull) = cb.doubleColumn(p.attribute)
      var i = 0
      while (i < n) {
        mask(i) = !isNull(i) && cmp(java.lang.Double.compare(vals(i), c))
        i += 1
      }
    } else {
      // General path: OR the compiled per-predicate tests over the rows.
      var i = 0
      while (i < n) {
        val t = batch(i)
        var keep = false
        var k = 0
        while (!keep && k < compiled.length) { keep = compiled(k)(t); k += 1 }
        mask(i) = keep
        i += 1
      }
    }
    Some(cb.select(mask).map(t => (t, None)))
  }

  private def isNumericSimd(sample: Tuple): Boolean = {
    val p = desc.predicates.head
    if (cmpOf(p.condition) == null) return false
    val tpe = try sample.getSchema.getAttribute(p.attribute).getType catch { case _: Throwable => return false }
    (tpe == AttributeType.INTEGER || tpe == AttributeType.DOUBLE) &&
    (try { p.value.toDouble; true } catch { case _: NumberFormatException => false })
  }

  // ---- Native-Arrow path (M2): consume the Arrow batch directly, no tuple decode.
  @transient private var columnarAllocator: RootAllocator = _

  override def processColumnarBatch(arrowIpcBytes: Array[Byte], port: Int): ColumnarResult = {
    if (!vectorizedEnabled || desc.predicates.size != 1) return ColumnarResult.Unsupported
    val p = desc.predicates.head
    val cmp = cmpOf(p.condition)
    if (cmp == null) return ColumnarResult.Unsupported
    val c =
      try p.value.toDouble
      catch { case _: NumberFormatException => return ColumnarResult.Unsupported }
    if (columnarAllocator == null) columnarAllocator = new RootAllocator()
    ArrowUtils.deserializeRootFold(arrowIpcBytes, columnarAllocator) { root =>
      val fields = root.getSchema.getFields
      var fieldIdx = -1
      var k = 0
      while (k < fields.size && fieldIdx < 0) {
        if (fields.get(k).getName == p.attribute) fieldIdx = k
        k += 1
      }
      if (fieldIdx < 0) ColumnarResult.Unsupported
      else {
        val n = root.getRowCount
        val mask = new Array[Boolean](n)
        root.getVector(fieldIdx) match {
          case v: Float8Vector =>
            var i = 0
            while (i < n) { mask(i) = !v.isNull(i) && cmp(java.lang.Double.compare(v.get(i), c)); i += 1 }
            ColumnarResult.Emit(ArrowUtils.selectRows(root, mask, columnarAllocator))
          case v: IntVector =>
            var i = 0
            while (i < n) {
              mask(i) = !v.isNull(i) && cmp(java.lang.Double.compare(v.get(i).toDouble, c)); i += 1
            }
            ColumnarResult.Emit(ArrowUtils.selectRows(root, mask, columnarAllocator))
          case _ => ColumnarResult.Unsupported // non-numeric column: fall back to the row path
        }
      }
    }
  }

  override def close(): Unit = {
    if (columnarAllocator != null) columnarAllocator.close()
  }
}
