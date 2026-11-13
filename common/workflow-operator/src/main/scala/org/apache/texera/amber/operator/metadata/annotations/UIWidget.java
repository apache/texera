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

public class UIWidget {

    public static final String UIWidgetTextArea = "{ \"widget\": {\n          \"formlyConfig\": {\n            \"type\": \"textarea\",\n            \"templateOptions\": {\n              \"autosize\": true,\n              \"autosizeMinRows\": 3\n            }\n          }\n        }\n      }";

    // A delimiter picker: common delimiters by name (so a tab is chosen, not typed), plus a
    // free-form entry for anything else. "char" is one literal character; "regex" is a pattern
    // the picker checks as it is typed.
    public static final String UIWidgetCharDelimiter = "{ \"maxLength\": 1, \"widget\": {\n          \"formlyConfig\": {\n            \"type\": \"delimiter\",\n            \"props\": {\n              \"delimiterMode\": \"char\"\n            }\n          }\n        }\n      }";

    public static final String UIWidgetRegexDelimiter = "{ \"widget\": {\n          \"formlyConfig\": {\n            \"type\": \"delimiter\",\n            \"props\": {\n              \"delimiterMode\": \"regex\"\n            }\n          }\n        }\n      }";

    public static final String UIWidgetPassword = "{ \"widget\": {\n          \"formlyConfig\": {\n            \"templateOptions\": {\n              \"type\": \"password\"\n            }\n          }\n        }\n      }";

}
