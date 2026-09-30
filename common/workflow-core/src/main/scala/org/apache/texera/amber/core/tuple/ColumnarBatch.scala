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

package org.apache.texera.amber.core.tuple

/**
  * Column-oriented view of a batch of tuples, for vectorized operator execution.
  * Wraps the original rows and extracts one column at a time into a contiguous
  * array, caching each extraction. A numeric column can be pulled into a
  * primitive `Array[Double]` plus a null mask for tight, JIT/SIMD-friendly loops.
  */
class ColumnarBatch(val rows: Array[Tuple]) {

  def size: Int = rows.length

  def schema: Schema = if (rows.isEmpty) null else rows(0).getSchema

  private val cache = scala.collection.mutable.HashMap[String, Array[Any]]()

  /** Column values in row order (boxed), extracted once and cached. */
  def column(name: String): Array[Any] = {
    cache.getOrElseUpdate(
      name, {
        val a = new Array[Any](rows.length)
        var i = 0
        while (i < rows.length) { a(i) = rows(i).getField[Any](name); i += 1 }
        a
      }
    )
  }

  /**
    * Numeric column as primitive doubles plus a per-row null mask. NaN is stored
    * where the value is null; callers must consult the mask. Only valid when the
    * column type is INTEGER or DOUBLE.
    */
  def doubleColumn(name: String): (Array[Double], Array[Boolean]) = {
    val vals = new Array[Double](rows.length)
    val isNull = new Array[Boolean](rows.length)
    var i = 0
    while (i < rows.length) {
      val f = rows(i).getField[Any](name)
      if (f == null) { isNull(i) = true }
      else vals(i) = f.asInstanceOf[Number].doubleValue()
      i += 1
    }
    (vals, isNull)
  }

  /** The rows selected by `mask` (mask.length must equal size), as an iterator. */
  def select(mask: Array[Boolean]): Iterator[Tuple] = {
    val out = scala.collection.mutable.ArrayBuffer[Tuple]()
    var i = 0
    while (i < rows.length) {
      if (mask(i)) out += rows(i)
      i += 1
    }
    out.iterator
  }
}
