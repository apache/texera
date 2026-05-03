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

package org.apache.texera.amber.tags

import org.scalatest.TagAnnotation

import java.lang.annotation.{ElementType, Retention, RetentionPolicy, Target}

/**
  * Class-level tag for tests that exercise both Scala and Python end-to-end.
  *
  * CI uses ScalaTest's `-n` / `-l` flags with the FQN
  * `org.apache.texera.amber.tags.IntegrationTest` to route these tests to the
  * dedicated `amber-integration` job (which provisions Python deps) and exclude
  * them from the lighter `amber` job. Apply at the class level — every test in
  * an annotated suite inherits the tag.
  */
@TagAnnotation
@Retention(RetentionPolicy.RUNTIME)
@Target(Array(ElementType.TYPE, ElementType.METHOD))
class IntegrationTest extends scala.annotation.StaticAnnotation
