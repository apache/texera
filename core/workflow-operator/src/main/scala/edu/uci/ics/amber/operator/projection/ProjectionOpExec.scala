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

package edu.uci.ics.amber.operator.projection

import com.google.common.base.Preconditions
import edu.uci.ics.amber.core.tuple.{Tuple, TupleLike}
import edu.uci.ics.amber.operator.map.MapOpExec
import edu.uci.ics.amber.util.JSONUtils.objectMapper

import scala.collection.mutable

class ProjectionOpExec(
    descString: String
) extends MapOpExec {

  val desc: ProjectionOpDesc = objectMapper.readValue(descString, classOf[ProjectionOpDesc])
  setMapFunc(project)

  def project(tuple: Tuple): TupleLike = {
    Preconditions.checkArgument(desc.attributes.nonEmpty)
    var selectedUnits: List[AttributeUnit] = List()
    val fields = mutable.LinkedHashMap[String, Any]()
    if (desc.isDrop) {
      val allAttribute = tuple.schema.getAttributeNames
      val selectedAttributes = desc.attributes.map(_.getOriginalAttribute)
      val keepAttributes = allAttribute.diff(selectedAttributes)

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

}
