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

class LoopStartOpDesc extends LoopOpDesc {
  @JsonProperty(required = true, defaultValue = "i = 0")
  @JsonSchemaTitle("Initialization")
  var initialization: EncodableString = ""

  @JsonProperty(required = true, defaultValue = "table.iloc[i]")
  @JsonSchemaTitle("Output")
  var output: EncodableString = ""

  override protected def operatorName: String = "Loop Start"

  override protected def operatorDescription: String =
    "Begin a loop that iterates over rows of the input table; pairs with Loop End."

  // The jump/write-back target of the loop's back-edge: the scheduler resolves
  // this operator's input-port state URI and ships it to workers at setup.
  override protected def isLoopStart: Boolean = true

  // User-supplied `initialization` and `output` are interpolated via the `pyb`
  // builder, which base64-encodes each EncodableString and renders it as a
  // `self.decode_python_template('<b64>')` expression. So an arbitrary user
  // string -- including quotes, newlines, or backslashes -- can never break the
  // surrounding Python syntax, because the text is no longer pasted in raw.
  override def generatePythonCode(): String = {
    pyb"""
       |from pytexera import *
       |class ProcessLoopStartOperator(LoopStartOperator):
       |    @overrides
       |    def open(self):
       |        self.state = {}
       |        exec($initialization, {}, self.state)
       |
       |    @overrides
       |    def process_table(self, table: Table, port: int) -> Iterator[Optional[TableLike]]:
       |        yield self.eval_output($output, table)
       |""".encode
  }
}
