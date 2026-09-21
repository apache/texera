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

package org.apache.texera.amber.operator.projection

import com.google.common.base.Preconditions
import org.apache.arrow.memory.RootAllocator
import org.apache.texera.amber.core.executor.{ColumnarOperatorExecutor, ColumnarResult}
import org.apache.texera.amber.core.tuple.{Attribute, Schema, Tuple, TupleLike}
import org.apache.texera.amber.operator.map.MapOpExec
import org.apache.texera.amber.util.ArrowUtils
import org.apache.texera.amber.util.JSONUtils.objectMapper

import scala.collection.mutable

class ProjectionOpExec(
    descString: String
) extends MapOpExec
    with ColumnarOperatorExecutor {

  val desc: ProjectionOpDesc = objectMapper.readValue(descString, classOf[ProjectionOpDesc])
  setMapFunc(project)

  def project(tuple: Tuple): TupleLike = {
    Preconditions.checkArgument(desc.attributes.nonEmpty)
    var selectedUnits: List[AttributeUnit] = List()
    val fields = mutable.LinkedHashMap[String, Any]()
    if (desc.isDrop) {
      val allAttribute = tuple.schema.getAttributeNames
      val selectedAttributes = desc.attributes.map(_.getOriginalAttribute.toLowerCase).toSet
      val keepAttributes =
        allAttribute.filterNot(attribute => selectedAttributes.contains(attribute.toLowerCase))

      keepAttributes.foreach { attribute =>
        val newList = List(
          new AttributeUnit(attribute, attribute)
        )
        selectedUnits = selectedUnits ::: newList
      }

    } else {

      selectedUnits = desc.attributes
    }

    selectedUnits.foreach { attributeUnit =>
      val alias = attributeUnit.getAlias
      if (fields.contains(alias)) {
        throw new RuntimeException("have duplicated attribute name/alias")
      }
      fields(alias) = tuple.getField[Any](attributeUnit.getOriginalAttribute)
    }

    TupleLike(fields.toSeq: _*)
  }

  // ---- Native-Arrow path: select (and rename) columns directly on the Arrow
  // batch, no per-row Tuple decode. Streaming 1:1, so it emits the projected batch.
  @transient private var columnarAllocator: RootAllocator = _

  override def processColumnarBatch(arrowIpcBytes: Array[Byte], port: Int): ColumnarResult = {
    Preconditions.checkArgument(desc.attributes.nonEmpty)
    if (columnarAllocator == null) columnarAllocator = new RootAllocator()
    ArrowUtils.deserializeRootFold(arrowIpcBytes, columnarAllocator) { root =>
      val full = ArrowUtils.toTexeraSchema(root.getSchema)
      // (originalName, alias) in output order, matching the row `project` path.
      val selected: List[(String, String)] =
        if (desc.isDrop) {
          val drop = desc.attributes.map(_.getOriginalAttribute).toSet
          full.getAttributeNames.filterNot(drop.contains).map(a => (a, a))
        } else desc.attributes.map(u => (u.getOriginalAttribute, u.getAlias))
      val aliases = selected.map(_._2)
      if (aliases.distinct.size != aliases.size) {
        throw new RuntimeException("have duplicated attribute name/alias")
      }
      val outSchema = Schema(selected.map {
        case (orig, alias) => new Attribute(alias, full.getAttribute(orig).getType)
      })
      val srcIdx = ArrowUtils.projectionIndices(root, selected.map(_._1))
      ColumnarResult.Emit(ArrowUtils.selectColumns(root, srcIdx, outSchema, columnarAllocator))
    }
  }

  override def close(): Unit = {
    if (columnarAllocator != null) { columnarAllocator.close(); columnarAllocator = null }
  }

}
