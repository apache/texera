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

package edu.uci.ics.amber.operator.source.scan.text

import edu.uci.ics.amber.core.executor.SourceOperatorExecutor
import edu.uci.ics.amber.core.tuple.AttributeTypeUtils.parseField
import edu.uci.ics.amber.core.tuple.TupleLike
import edu.uci.ics.amber.operator.source.scan.FileAttributeType
import edu.uci.ics.amber.util.JSONUtils.objectMapper

class TextInputSourceOpExec private[text] (
    descString: String
) extends SourceOperatorExecutor {
  private val desc: TextInputSourceOpDesc =
    objectMapper.readValue(descString, classOf[TextInputSourceOpDesc])
  override def produceTuple(): Iterator[TupleLike] = {
    (if (desc.attributeType.isSingle) {
       Iterator(desc.textInput)
     } else {
       desc.textInput.linesIterator.slice(
         desc.fileScanOffset.getOrElse(0),
         desc.fileScanOffset.getOrElse(0) + desc.fileScanLimit.getOrElse(Int.MaxValue)
       )
     }).map(line =>
      TupleLike(desc.attributeType match {
        case FileAttributeType.SINGLE_STRING => line
        case FileAttributeType.BINARY        => line.getBytes
        case _                               => parseField(line, desc.attributeType.getType)
      })
    )
  }

}
