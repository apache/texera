/**
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

import { Validators } from "@angular/forms";
import { FormlyFieldConfig } from "@ngx-formly/core";

// Formly attaches per-form runtime state to field-config objects, so every
// consumer must get a fresh copy instead of sharing one array.
export function contributorFieldGroup(): FormlyFieldConfig[] {
  return [
    {
      key: "name",
      type: "input",
      defaultValue: "",
      templateOptions: {
        label: "Name",
        required: true,
        placeholder: "Enter name",
        maxLength: 256,
      },
    },
    {
      key: "creator",
      type: "checkbox",
      defaultValue: false,
      templateOptions: {
        label: "Creator",
      },
    },
    {
      key: "email",
      type: "input",
      defaultValue: "",
      templateOptions: {
        label: "Email",
        type: "email",
        placeholder: "Enter email",
        maxLength: 256,
      },
      validators: {
        validation: [Validators.email],
      },
      validation: {
        messages: {
          email: "Please enter a valid email address",
        },
      },
    },
    {
      key: "affiliation",
      type: "input",
      defaultValue: "",
      templateOptions: {
        label: "Affiliation",
        placeholder: "Enter affiliation",
        maxLength: 256,
      },
    },
    {
      key: "comments",
      type: "textarea",
      defaultValue: "",
      templateOptions: {
        label: "Comments",
        placeholder: "Additional information",
        rows: 3,
        maxLength: 500,
        attributes: {
          style: "resize: none",
        },
      },
    },
  ];
}
