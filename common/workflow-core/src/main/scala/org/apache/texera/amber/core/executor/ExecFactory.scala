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

package org.apache.texera.amber.core.executor

import org.apache.texera.amber.core.state.StateReferenceModule

object ExecFactory {

  def newExecFromJavaCode(code: String): OperatorExecutor = {
    JavaRuntimeCompilation
      .compileCode(code)
      .getDeclaredConstructor()
      .newInstance()
      .asInstanceOf[OperatorExecutor]
  }

  /**
    * A descriptor whose `stateReferences` sidecar names loop variables gets the operator's own
    * executor, built from the descString with its placeholders like any other; the descriptor its
    * constructor parses is its setting, which each state message registered on it writes the loop
    * variables into (`OperatorExecutor.registerState`). The compiler fills the sidecar in only
    * inside a loop block, so a `$name` string anywhere else is the literal it looks like.
    */
  def newExecFromJavaClassName[K](
      className: String,
      descString: String = "",
      idx: Int = 0,
      workerCount: Int = 1
  ): OperatorExecutor = {
    val references = StateReferenceBinding.sidecarOf(descString)
    if (references.isEmpty) {
      instantiate[K](className, descString, idx, workerCount)
    } else {
      val (executor, settings) =
        StateReferenceModule.capturing(instantiate[K](className, descString, idx, workerCount))
      def refuse(reason: String): Nothing = {
        val named = references.toSeq.sorted.map { case (pointer, name) => s"$pointer -> $$$name" }
        throw new IllegalStateException(
          s"$className refers to loop variables (${named.mkString(", ")}), but its constructor " +
            s"parsed $reason"
        )
      }
      settings match {
        case List(setting) =>
          OperatorExecutor.attach(
            executor,
            new StateReferenceBinding(className, setting, references)
          )
        case Nil => refuse("no descriptor from its descString to write them into")
        case _ =>
          refuse(
            s"${settings.size} descriptors from its descString, so which one is its setting is unclear"
          )
      }
      executor
    }
  }

  private def instantiate[K](
      className: String,
      descString: String,
      idx: Int,
      workerCount: Int
  ): OperatorExecutor = {
    val clazz = Class.forName(className).asInstanceOf[Class[K]]
    try {
      if (descString.isEmpty) {
        clazz.getDeclaredConstructor().newInstance().asInstanceOf[OperatorExecutor]
      } else {
        clazz
          .getDeclaredConstructor(classOf[String])
          .newInstance(descString)
          .asInstanceOf[OperatorExecutor]
      }
    } catch {
      case e: NoSuchMethodException =>
        if (descString.isEmpty) {
          clazz
            .getDeclaredConstructor(classOf[Int], classOf[Int])
            .newInstance(idx, workerCount)
            .asInstanceOf[OperatorExecutor]
        } else {
          clazz
            .getDeclaredConstructor(classOf[String], classOf[Int], classOf[Int])
            .newInstance(descString, idx, workerCount)
            .asInstanceOf[OperatorExecutor]
        }
    }
  }
}
