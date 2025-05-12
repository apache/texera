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

import { Component } from "@angular/core";
import { FieldArrayType } from "@ngx-formly/core";

@Component({
  template: `
    <hr />
    <div
      *ngFor="let field of field.fieldGroup; let i = index"
      style="margin: 0;">
      <formly-field
        [field]="field"
        style="padding-left: 0;display:inline-block;width:calc(100% - 24px)"
        class="dynamic-fields"></formly-field>
      <button
        nz-button
        [nzSize]="'small'"
        [nzShape]="'circle'"
        nzDanger
        type="button"
        (click)="remove(i)">
        <span
          nz-icon
          nzType="delete"></span>
      </button>
      <hr />
    </div>
    <h4 style="display:inline-block;">{{ props.label }}</h4>
    <button
      nz-button
      [nzSize]="'small'"
      [nzType]="'primary'"
      [nzShape]="'circle'"
      type="button"
      (click)="add()"
      style="display:inline-block;vertical-align: baseline;float: right;">
      <span
        nz-icon
        nzType="plus"></span>
    </button>
  `,
})
export class ArrayTypeComponent extends FieldArrayType {}
/**  Copyright 2018 Google Inc. All Rights Reserved.
 Use of this source code is governed by an MIT-style license that
 can be found in the LICENSE file at http://angular.io/license */
