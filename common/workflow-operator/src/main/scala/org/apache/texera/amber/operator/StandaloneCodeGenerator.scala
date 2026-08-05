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

package org.apache.texera.amber.operator

trait StandaloneCodeGenerator {

  def generateStandaloneCode(): String

  def producesDataFrame(): Boolean = true

  /**
    * Definitions this operator's standalone code depends on, emitted once near
    * the top of the script rather than inline.
    *
    * The translator concatenates operator bodies into a single module, so an
    * operator needing a helper class has nowhere to put it that another operator
    * would not duplicate. Helpers returned here are collected across the whole
    * plan and deduplicated by their text, so two sampling operators in one
    * workflow yield one copy of the generator they share.
    */
  def standaloneHelpers(): Seq[String] = Seq.empty
}
