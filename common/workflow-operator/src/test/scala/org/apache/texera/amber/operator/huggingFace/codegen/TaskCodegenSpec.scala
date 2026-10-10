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

package org.apache.texera.amber.operator.huggingFace.codegen

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
  * Covers the one piece of behavior the TaskCodegen trait implements itself:
  * the default `tasks` set.
  *
  * Reachability, stated plainly: no production path executes this default today.
  * The only four `.tasks` call sites in main are HuggingFaceInferenceOpDesc's
  * dispatcher registrations (lines 175-178), and all four are on codegens that
  * declare `override val tasks`; TextGenCodegen, the sole single-task codegen, is
  * registered by its `task` string and never asks for `tasks`. This suite
  * therefore documents an extension point for future single-task codegens rather
  * than guarding a shipped path.
  */
class TaskCodegenSpec extends AnyFlatSpec with Matchers {

  /**
    * Implements only the three abstract members, leaving `tasks` to the trait.
    *
    * `task` is declared `override val` to match every shipped codegen
    * (AudioTaskCodegen, ImageTaskCodegen, MediaGenCodegen, QaRankingCodegen and
    * TextGenCodegen all use `override val task`). The shape matters: were the
    * trait's `tasks` ever turned into a `val`, it would be initialized during
    * trait construction — before the implementation's `val task` is assigned —
    * and would capture `Set(null)`. A stub written with `override def task` is
    * immune to that initialization-order hazard and so would not notice it.
    */
  private object SingleTaskStub extends TaskCodegen {
    override val task: String = "probe-task-zX7q42"
    override def payloadPython(ctx: CodegenContext): String = ""
    override def parsePython(ctx: CodegenContext): String = ""
  }

  "TaskCodegen.tasks" should "default to the singleton set of the codegen's own task" in {
    // The stub's task string is deliberately not one of the real Hugging Face
    // pipeline names, so a default hardcoded to some shipped task string rather
    // than derived from `task` cannot pass this.
    SingleTaskStub.tasks shouldBe Set("probe-task-zX7q42")
  }
}
