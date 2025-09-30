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

package edu.uci.ics.amber.core.executor

object ExecFactory {

  def newExecFromJavaCode(code: String): OperatorExecutor = {
    JavaRuntimeCompilation
      .compileCode(code)
      .getDeclaredConstructor()
      .newInstance()
      .asInstanceOf[OperatorExecutor]
  }

  def newExecFromJavaClassName[K](
      className: String,
      descString: String = "",
      idx: Int = 0,
      workerCount: Int = 1
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
