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
import org.apache.arrow.vector.VectorSchemaRoot
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

  // ---- Native-Arrow path: project columns straight on the Arrow batch, no per-tuple
  // decode. Each output column takes its source column's buffers by an Arrow transfer
  // (a move, not a per-cell copy), renamed to the alias. Falls back to the row path
  // (Unsupported) for the shapes a single column move cannot express: a source column
  // used by more than one output, a duplicated output name, or an unresolved column.
  @transient private var columnarAllocator: RootAllocator = _

  override def processColumnarBatch(arrowIpcBytes: Array[Byte], port: Int): ColumnarResult = {
    if (columnarAllocator == null) columnarAllocator = new RootAllocator()
    ArrowUtils.deserializeRootFold(arrowIpcBytes, columnarAllocator)(projectRoot)
  }

  // Resolve the (sourceColumn -> outputAlias) pairs exactly as project() does, then move
  // each source column into a fresh output root under its alias.
  private def projectRoot(root: VectorSchemaRoot): ColumnarResult = {
    val inSchema = ArrowUtils.toTexeraSchema(root.getSchema)
    val units: List[(String, String)] =
      if (desc.isDrop) {
        val dropped = desc.attributes.map(_.getOriginalAttribute.toLowerCase).toSet
        inSchema.getAttributeNames.filterNot(n => dropped.contains(n.toLowerCase)).map(n => (n, n))
      } else {
        desc.attributes.map(u => (u.getOriginalAttribute, u.getAlias))
      }
    val originals = units.map(_._1)
    val aliases = units.map(_._2)
    // A source column used twice cannot be moved twice; a duplicated alias is an error
    // the row path raises. Let the row path handle both.
    if (
      units.isEmpty || originals.distinct.size != originals.size ||
      aliases.distinct.size != aliases.size
    ) {
      return ColumnarResult.Unsupported
    }
    val outSchema =
      try Schema(units.map {
        case (orig, alias) => new Attribute(alias, inSchema.getAttribute(orig).getType)
      })
      catch { case _: Exception => return ColumnarResult.Unsupported }
    val srcIdx =
      try ArrowUtils.projectionIndices(root, originals)
      catch { case _: Exception => return ColumnarResult.Unsupported }
    val outRoot = VectorSchemaRoot.create(ArrowUtils.fromTexeraSchema(outSchema), columnarAllocator)
    var j = 0
    while (j < units.size) {
      // Move the source column's buffers into the output column (same Arrow type), renamed.
      root.getVector(srcIdx(j)).makeTransferPair(outRoot.getVector(j)).transfer()
      j += 1
    }
    outRoot.setRowCount(root.getRowCount)
    ColumnarResult.Emit(outRoot)
  }

  override def close(): Unit = {
    if (columnarAllocator != null) { columnarAllocator.close(); columnarAllocator = null }
  }

}
