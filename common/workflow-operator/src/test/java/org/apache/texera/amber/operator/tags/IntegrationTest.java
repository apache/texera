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

package org.apache.texera.amber.operator.tags;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.scalatest.TagAnnotation;

/**
 * Marks a test in this module as needing more than a bare Python interpreter —
 * pandas or plotly, which the {@code amber} job does not install. See the
 * AMBER_TEST_FILTER block in {@code common/workflow-operator/build.sbt} for how
 * it routes to {@code amber-integration}.
 *
 * <p>Apply it to a whole spec as an annotation, or — so that a spec's cheaper
 * assertions stay in the unit job and its coverage report — to a single case:
 * {@code test(name, Tag(classOf[IntegrationTest].getName))} in a FunSuite,
 * {@code taggedAs} in a FlatSpec.
 *
 * <p>amber's own tag lives in {@code amber/src/test/integration} and is not
 * reachable here, since amber depends on this module rather than the reverse.
 *
 * <p>Java, not Scala: ScalaTest finds tag annotations by
 * {@code java.lang.annotation} reflection, and a Scala {@code StaticAnnotation}
 * produces no JVM annotation interface for {@code @TagAnnotation} to mark.
 */
@TagAnnotation
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD, ElementType.TYPE})
public @interface IntegrationTest {
}
