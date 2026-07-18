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

package org.apache.texera.amber.operator.loop

import com.fasterxml.jackson.annotation.JsonProperty
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle
import org.apache.texera.amber.pybuilder.PyStringTypes.EncodableString
import org.apache.texera.amber.pybuilder.PythonTemplateBuilder.PythonTemplateBuilderStringContext

class LoopEndOpDesc extends LoopOpDesc {
  @JsonProperty(required = true, defaultValue = "i += 1")
  @JsonSchemaTitle("Update")
  var update: EncodableString = ""

  @JsonProperty(required = true, defaultValue = "i < len(table)")
  @JsonSchemaTitle("Condition")
  var condition: EncodableString = ""

  override protected def operatorName: String = "Loop End"

  override protected def operatorDescription: String =
    "Close a loop body and decide whether to iterate again based on a condition; pairs with Loop Start."

  override protected def reuseStorage: Boolean = true

  // `update` and `condition` are base64-wrapped by `pyb`; see
  // LoopOpDesc.generatePythonCode.
  override def generatePythonCode(): String = {
    pyb"""
       |from pytexera import *
       |class ProcessLoopEndOperator(LoopEndOperator):
       |    @overrides
       |    def process_state(self, state: State, port: int) -> Optional[State]:
       |        self.run_update($update, state)
       |        return None
       |
       |    @overrides
       |    def condition(self) -> bool:
       |        return self.eval_condition($condition)
       |""".encode
  }
}
