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

package org.apache.texera.amber.core.state;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a descriptor property that the compiled plan is built from: the name or type of an output
 * column, or what an input is partitioned on. The compiler fixes the plan before any loop runs, so
 * the property cannot refer to a loop variable, nor can anything inside it: the loop state would
 * write the value into the operator's setting after the schema or the partitioning was already
 * built from the placeholder. Inside a loop block such a reference is a compile error
 * ({@code WorkflowCompiler.normalizeStateReferences}); outside every block a {@code $name} string
 * stays the literal it is.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.FIELD})
public @interface FixedAtCompileTime {
}
