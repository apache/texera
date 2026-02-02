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

package org.apache.texera.amber.engine.architecture.worker.promisehandlers

import com.twitter.util.Future
import org.apache.texera.amber.engine.architecture.rpc.controlcommands.{AsyncRPCContext, UpdateExecutorRequest}
import org.apache.texera.amber.engine.architecture.rpc.controlreturns.EmptyReturn
import org.apache.texera.amber.engine.architecture.worker.DataProcessorRPCHandlerInitializer
import scala.reflect.runtime.universe._

trait UpdateExecutorHandler {
  this: DataProcessorRPCHandlerInitializer =>

  override def updateExecutor(
                               request: UpdateExecutorRequest,
                               ctx: AsyncRPCContext
                           ): Future[EmptyReturn] = {
    val oldOpExecState = dp.executor
    initializeExecutor(request.newExecInitInfo)
    dp.executor.open()
    copyMatchingFields(oldOpExecState, dp.executor) //TBD if we really need this
    EmptyReturn()
  }

  private[this] def copyMatchingFields[A: TypeTag, B: TypeTag](from: A, to: B): Unit = {
    val mirror = runtimeMirror(from.getClass.getClassLoader)

    val fromFields = typeOf[A].members.collect {
      case m: MethodSymbol if m.isGetter => m
    }

    val toFields = typeOf[B].members.collect {
      case m: MethodSymbol if m.isVar => m
    }.map(m => m.name.toString -> m).toMap

    fromFields.foreach { f =>
      val name = f.name.toString
      toFields.get(name).foreach { setter =>
        if (f.returnType =:= setter.returnType) {
          val value = mirror.reflect(from).reflectMethod(f).apply()
          mirror.reflect(to).reflectField(setter).set(value)
        }
      }
    }
  }
}
