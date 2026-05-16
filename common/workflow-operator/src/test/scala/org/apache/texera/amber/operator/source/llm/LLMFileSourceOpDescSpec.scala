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

package org.apache.texera.amber.operator.source.llm

import org.apache.texera.amber.core.tuple.{Attribute, AttributeType}
import org.apache.texera.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.jdk.CollectionConverters._

class LLMFileSourceOpDescSpec extends AnyFlatSpec with Matchers {

  private val workflowId = WorkflowIdentity(1L)
  private val executionId = ExecutionIdentity(1L)

  "LLMFileSourceOpDesc.getPhysicalOp" should "densify sparse per-table rows to the union schema" in {
    val desc = new LLMFileSourceOpDesc
    desc.generatedCode =
      """from pytexera import *
        |
        |class GenerateOperator(UDFSourceOperator):
        |    @overrides
        |    def produce(self):
        |        yield {"__table__": "revenue_by_region", "month": "January"}
        |""".stripMargin
    desc.unionColumns = List(
      new Attribute("__table__", AttributeType.STRING),
      new Attribute("month", AttributeType.STRING),
      new Attribute("department", AttributeType.STRING)
    ).asJava

    val code = desc.getPhysicalOp(workflowId, executionId).getCode

    code should include("""_texera_llm_source_columns = ["__table__","month","department"]""")
    code should include("yield {column: row.get(column) for column in _texera_llm_source_columns}")
  }
}
