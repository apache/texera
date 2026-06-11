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

package org.apache.texera.amber.operator.source.apis.twitter

import org.apache.texera.amber.core.executor.SourceOperatorExecutor

@deprecated("Twitter source operator is no longer executable.", "1.1.0-incubating")
abstract class TwitterSourceOpExec(
    descString: String
) extends SourceOperatorExecutor {
  override def open(): Unit =
    throw new UnsupportedOperationException(
      "Twitter source operator is no longer executable in Apache Texera."
    )

  override def close(): Unit = {}
}
