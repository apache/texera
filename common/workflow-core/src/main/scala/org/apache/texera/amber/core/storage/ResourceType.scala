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

package org.apache.texera.amber.core.storage

/**
  * The leading segment of a logical file path, identifying which resource kind (and thus which
  * backing table) a path belongs to.
  *
  * Path shape: /<prefix>/ownerEmail/resourceName/versionName/fileRelativePath
  */
object ResourceType extends Enumeration {
  val Datasets: Value = Value("datasets")

  /**
    * Returns true if the given path segment names a known resource type.
    * Used to validate the leading prefix of a logical path.
    */
  def isValidPrefix(segment: String): Boolean = values.exists(_.toString == segment)
}
