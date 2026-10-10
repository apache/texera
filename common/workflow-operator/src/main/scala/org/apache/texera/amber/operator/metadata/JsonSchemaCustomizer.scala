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

package org.apache.texera.amber.operator.metadata

import com.fasterxml.jackson.databind.node.ObjectNode

/**
  * An operator with part of its schema that annotations cannot express, because that part
  * depends on the descriptor's own type argument rather than on any one field.
  *
  * [[OperatorMetadataGenerator.generateOperatorJsonSchema]] calls this once the annotated
  * schema is built, so an implementor edits a finished document rather than producing one.
  */
trait JsonSchemaCustomizer {

  /** Add to `schema` what the annotations could not state. Called with the operator's whole
    * schema, `definitions` included.
    */
  def customizeJsonSchema(schema: ObjectNode): Unit
}
