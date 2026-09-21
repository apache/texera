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

package org.apache.texera.amber.operator.metadata.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Test-only metadata for transform verification: names the column in the shared
 * verification fixture the operator runs on that should fill this
 * {@code @AutofillAttributeName} field when the operator is auto-configured.
 *
 * <p>It lets a field declare a <em>semantic</em> sample that the column's
 * {@code AttributeType} alone cannot express — e.g. a valid three-letter ISO
 * country code, or a genuine OHLC price column — so the parity test exercises
 * the operator on realistic input instead of a degenerate first-column pick
 * (which can hide translation bugs and produce vacuous passes).
 *
 * <p>This has <em>no effect on production</em>: it is not a Jackson / JSON-schema
 * annotation and is read only by the test-side ConfigGenerator.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.FIELD})
public @interface SampleColumn {
    String value();
}
