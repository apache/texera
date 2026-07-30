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
 * Test-only metadata for transform verification: marks a string field as a CSS
 * color, and names the sample the parity test should fill it with.
 *
 * <p>A color is the one free-text knob whose value the generic fill cannot guess:
 * the canonical string is not a color, and plotly refuses it outright
 * ({@code Invalid value of type 'builtins.str' received for the 'color'
 * property}), so the branch that reads the knob is never reached. Nothing else in
 * the metadata says "this string is a color" — unlike a column, which
 * {@code @AutofillAttributeName} marks — and guessing from the field name misses a
 * knob called {@code background} or {@code stroke}.
 *
 * <p>The marker carries no value: the forms a user can type — a named color, a
 * 3-digit hex, a full hex, {@code rgb(...)}, {@code rgba(...)} — live in one list on
 * the test side and are swept over this field one at a time, the way a declared enum
 * is. So every color knob sees every form, and no single form is picked per field.
 *
 * <p>This has <em>no effect on production</em>: it is not a Jackson / JSON-schema
 * annotation and is read only by the test-side ConfigGenerator.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.FIELD})
public @interface ColorValue {}
