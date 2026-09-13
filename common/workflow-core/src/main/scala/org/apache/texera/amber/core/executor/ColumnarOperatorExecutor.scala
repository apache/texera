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

import org.apache.arrow.vector.VectorSchemaRoot

/**
  * An operator that can consume an Arrow columnar batch directly (no per-row
  * Tuple decode) and produce one. Given the incoming batch as Arrow IPC bytes,
  * returns the result batch, or None if this batch shape is not supported (the
  * caller then falls back to the row path). The returned root is owned by the
  * caller, which serializes and closes it.
  */
trait ColumnarOperatorExecutor {
  def processColumnarBatch(arrowIpcBytes: Array[Byte]): Option[VectorSchemaRoot]
}
