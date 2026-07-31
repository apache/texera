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

package org.apache.texera.common.tags;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.scalatest.TagAnnotation;

/**
 * Class-level marker for ScalaTest specs that must not run concurrently with one another.
 * {@code common/workflow-core/build.sbt} reflects over the tagged suites and gives each its own
 * forked JVM via {@code Test/testGrouping}; sbt runs forked groups one at a time (the
 * {@code Tags.ForkedTestGroup} limit), so tagged suites are serialized while every untagged suite
 * runs in parallel in a single shared group.
 *
 * <p>Use this for suites that share a JVM-wide singleton backed by an external resource and would
 * otherwise contend when the ScalaTest distributor runs them in parallel — e.g. the MinIO-backed
 * suites mixing {@code S3StorageTestBase}, which share one {@code S3StorageClient.s3Client} and
 * {@code StorageConfig} endpoint (see issue #7049).
 *
 * <p>Written in Java rather than Scala on purpose: the annotation must be visible through
 * {@code java.lang.annotation} reflection (it is discovered by {@code isAnnotationPresent} at
 * build time, and {@code @TagAnnotation} makes it a first-class ScalaTest tag). A Scala
 * {@code class extends StaticAnnotation} does not produce a runtime-retained JVM annotation, so it
 * would be invisible.
 */
@TagAnnotation
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE})
public @interface NonParallelTest {
}
