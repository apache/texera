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

package org.apache.texera.tags;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.scalatest.TagAnnotation;

/**
 * Shared class-level marker tag for ScalaTest specs that need real external
 * infrastructure — e.g. a Postgres instance (embedded or otherwise) — as
 * opposed to pure in-memory unit tests. Modules can filter on it with
 * ScalaTest's {@code -l}/{@code -n} arguments
 * ({@code org.apache.texera.tags.IntegrationTest}) to split unit and
 * integration CI legs; a module that wires no filter simply runs the tagged
 * specs with the rest of its suite.
 *
 * <p>The amber module predates this shared tag and still carries its own
 * {@code org.apache.texera.amber.tags.IntegrationTest} for the
 * amber/amber-integration job split; new specs outside amber should use this
 * one.
 *
 * <p>Written in Java rather than Scala because ScalaTest detects tag
 * annotations via {@code java.lang.annotation} reflection. A Scala
 * {@code class extends StaticAnnotation} does not produce a JVM
 * annotation interface that {@code @TagAnnotation} can attach to, so
 * the tag would be invisible to ScalaTest at runtime.
 */
@TagAnnotation
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD, ElementType.TYPE})
public @interface IntegrationTest {
}
