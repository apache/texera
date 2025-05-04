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

package edu.uci.ics.amber.util.serde
import edu.uci.ics.amber.core.virtualidentity.{OperatorIdentity, PhysicalOpIdentity}
import edu.uci.ics.amber.core.workflow.{GlobalPortIdentity, PortIdentity}

/**
  * Serialize and deserializes a GlobalPortIdentity object to a string using a custom, human-readable format
  * to ensure it works with both URI and file path and does not incldue underscore "_" so that it does not
  * interfere with our own VFS URI parsing.
  */
object GlobalPortIdentitySerde {
  implicit class SerdeOps(globalPortId: GlobalPortIdentity) {

    /**
      * Serializes a GlobalPortIdentity object into a string using our custom, human-readable format
      * that works with both URI and file path and does not incldue underscore "_" so that it does not
      * interfere with our own VFS URI parsing.
      *
      * @return A serialized string representation of globalPortId
      */
    def serializeAsString: String = {
      val logicalOpId = globalPortId.opId.logicalOpId.id
      val layerName = globalPortId.opId.layerName
      val portId = globalPortId.portId.id
      val isInternal = globalPortId.portId.internal
      val isInput = globalPortId.input
      s"(logicalOpId=$logicalOpId,layerName=$layerName,portId=$portId,isInternal=$isInternal,isInput=$isInput)"
    }
  }

  /**
    * Deserializes a string as a GlobalPortIdentity object. Must use our custom format:
    * `(logicalOpId=<logicalOpId>,layerName=<layerName>,portId=<portId.id>,isInternal=<portId.internal>,isInput=<input>)`
    * @param serializedGlobalPortId A serialized string foramt of a GlobalPortIdentity
    * @return A desrialized GlobalPortIdentity, or IllegalArgumentException if the format is not correct.
    */
  def deserializeFromString(serializedGlobalPortId: String): GlobalPortIdentity = {
    val pattern =
      """\(logicalOpId=([^,]+),layerName=([^,]+),portId=([^,]+),isInternal=([^,]+),isInput=([^)]+)\)""".r
    serializedGlobalPortId match {
      case pattern(logicalOpId, layerName, portIdStr, isInternalStr, isInputStr) =>
        val portIdInt = portIdStr.toInt
        val isInternal = isInternalStr.toBoolean
        val isInput = isInputStr.toBoolean
        val physicalOpId = PhysicalOpIdentity(
          logicalOpId = OperatorIdentity(logicalOpId),
          layerName = layerName
        )
        val portId = PortIdentity(id = portIdInt, internal = isInternal)
        GlobalPortIdentity(opId = physicalOpId, portId = portId, input = isInput)
      case _ =>
        throw new IllegalArgumentException(
          s"Invalid GlobalPortIdentity format: $serializedGlobalPortId"
        )
    }
  }
}
