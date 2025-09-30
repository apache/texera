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

package edu.uci.ics.amber.operator.metadata.annotations;

/* For every hide annotation, you specify on a formly field three things:
    the target (field name of dependent comparison),
    the hide type (specified by Type),
    and the expected value (specified as a string).

    This information is passed to the frontend and hidden.
    Here's how you specify an example hide, hiding someOtherFieldName when someFieldName == 3:
    @JsonSchemaInject(strings = {
            @JsonSchemaString(path = HideAnnotation.hideTarget, value = "someFieldName"),
            @JsonSchemaString(path = HideAnnotation.hideType, value = HideAnnotation.Type.equals),
            @JsonSchemaString(path = HideAnnotation.hideExpectedValue, value = "3")
    })
    public Integer someOtherFieldName;

    public Integer someFieldName;
 */
public class HideAnnotation {
    public final static String hideTarget = "hideTarget";
    public final static String hideType = "hideType";
    public final static String hideExpectedValue = "hideExpectedValue";
    public final static String hideOnNull = "hideOnNull";

    /* The types of matching on which a hide occurs. Evaluated at runtime by javascript. */
    public static class Type {
        /* String equality operator is applied to assert hideTarget == hideExpectedValue. */
        public final static String equals = "equals";

        /* Regex matching is applied to assert regex for hideExpectedValue matches hideTarget */
        public final static String regex = "regex";
    }
}