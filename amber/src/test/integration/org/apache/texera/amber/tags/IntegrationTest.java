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

package org.apache.texera.amber.tags;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.scalatest.TagAnnotation;

/**
 * Class-level marker tag for ScalaTest specs that exercise both Scala
 * and Python end-to-end. Routing to the {@code amber-integration} CI
 * job is by ScalaTest tag filtering, controlled by the
 * {@code AMBER_TEST_FILTER} env var in {@code amber/build.sbt}: the
 * lighter {@code amber} job runs with {@code skip-integration} (which
 * passes {@code -l org.apache.texera.amber.tags.IntegrationTest} to
 * ScalaTest), and the {@code amber-integration} job runs with
 * {@code integration-only} (which passes {@code -n} for the same tag).
 * The {@code amber/src/test/integration} directory is added to sbt's
 * {@code Test/unmanagedSourceDirectories} so these specs compile in
 * the regular Test config; there is no separate sbt configuration.
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
