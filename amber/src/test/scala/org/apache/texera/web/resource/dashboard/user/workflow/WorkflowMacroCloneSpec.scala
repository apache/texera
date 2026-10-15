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

package org.apache.texera.web.resource.dashboard.user.workflow

import com.fasterxml.jackson.databind.ObjectMapper
import org.scalatest.flatspec.AnyFlatSpec

class WorkflowMacroCloneSpec extends AnyFlatSpec {
  private val mapper = new ObjectMapper()

  "Cloning a workflow" should "rewrite canvas references and preserve pinned dependencies" in {
    val content = mapper.readTree("""{
      "operators":[{"operatorID":"leaf","operatorType":"Filter"}],
      "operatorPositions":{"leaf":{"x":0,"y":0}},
      "formBinding":{"fields":[{"operatorID":"leaf","propertyKey":"value"}]},
      "macroInterface":{"id":"definition","inputs":[{"targets":[{"operatorID":"leaf","portID":"input-0"}]}],"outputs":[]},
      "macroDependencies":[{"id":"child","version":8,"content":{"operators":[{"operatorID":"leaf","operatorType":"Filter"}]}}]
    }""")
    val cloned = mapper.readTree(WorkflowResource.assignNewOperatorIds(content.toString))
    val id = cloned.at("/operators/0/operatorID").asText()
    assert(id != "leaf")
    assert(cloned.path("operatorPositions").has(id))
    assert(cloned.at("/formBinding/fields/0/operatorID").asText() == id)
    assert(cloned.at("/macroInterface/inputs/0/targets/0/operatorID").asText() == id)
    assert(cloned.at("/macroInterface/id").asText() != "definition")
    assert(cloned.path("macroDependencies") == content.path("macroDependencies"))
  }

  it should "keep ordinary and empty workflows valid and reject malformed content" in {
    assert(
      mapper
        .readTree(WorkflowResource.assignNewOperatorIds("""{"operators":[]}"""))
        .path("operators")
        .isEmpty
    )
    intercept[Exception](WorkflowResource.assignNewOperatorIds("{}"))
    intercept[Exception](WorkflowResource.assignNewOperatorIds("invalid"))
  }

  it should "rewrite only graph references when IDs occur in macro types and user values" in {
    val content = mapper.readTree("""{
      "operators":[{"operatorID":"leaf","operatorType":"WorkflowMacro:leaf-definition:8","operatorProperties":{"code":"leaf + 1","value":"leaf"},"customDisplayName":"leaf"}],
      "operatorPositions":{"leaf":{"x":0,"y":0}},
      "links":[{"source":{"operatorID":"leaf","portID":"output-0"},"target":{"operatorID":"external","portID":"input-0"}}],
      "formBinding":{"fields":[{"id":"leaf-field","operatorID":"leaf","propertyKey":"leaf-key"}],"shownResultIds":["leaf"]},
      "macroDependencies":[{"id":"leaf-definition","version":8,"content":{"operators":[]}}]
    }""")
    val cloned = mapper.readTree(WorkflowResource.assignNewOperatorIds(content.toString))
    val id = cloned.at("/operators/0/operatorID").asText()
    assert(id != "leaf")
    assert(cloned.at("/operators/0/operatorType") == content.at("/operators/0/operatorType"))
    assert(
      cloned.at("/operators/0/operatorProperties") == content.at("/operators/0/operatorProperties")
    )
    assert(cloned.at("/operators/0/customDisplayName").asText() == "leaf")
    assert(cloned.at("/formBinding/fields/0/id").asText() == "leaf-field")
    assert(cloned.at("/formBinding/fields/0/propertyKey").asText() == "leaf-key")
    assert(cloned.at("/formBinding/shownResultIds/0").asText() == id)
    assert(cloned.at("/links/0/source/operatorID").asText() == id)
    assert(cloned.at("/links/0/target/operatorID").asText() == "external")
    assert(cloned.path("macroDependencies") == content.path("macroDependencies"))
  }
}
