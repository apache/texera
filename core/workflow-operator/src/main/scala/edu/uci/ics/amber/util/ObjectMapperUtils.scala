/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.uci.ics.amber.util

import edu.uci.ics.amber.operator.metadata.OperatorMetadataGenerator

object ObjectMapperUtils {

  /**
    * Explicitly start a thread to let the objectMapper load all logical operator classes for serde/deserde.
    *
    * This call prevents the initial delay of serialization & deserialization in other application logics.
    */
  def warmupObjectMapperForOperatorsSerde(): Unit = {
    val thread = new Thread(
      new Runnable {
        override def run(): Unit = {
          OperatorMetadataGenerator.generateAllOperatorMetadata()
        }
      },
      "ObjectMapperWarmupForOperatorsThread"
    )
    thread.start()
  }
}
