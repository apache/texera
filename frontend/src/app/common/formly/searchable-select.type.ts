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

import { CommonModule } from "@angular/common";
import { Component } from "@angular/core";
import { ReactiveFormsModule } from "@angular/forms";
import { FieldType, FieldTypeConfig, FormlyModule } from "@ngx-formly/core";
import { FormlyFieldSelectProps, FormlySelectModule } from "@ngx-formly/core/select";
import { NzSelectModule } from "ng-zorro-antd/select";

interface SearchableSelectProps extends FormlyFieldSelectProps {
  multiple?: boolean;
}

@Component({
  template: `
    <nz-select
      [class.ng-dirty]="showError"
      [nzPlaceHolder]="props.placeholder ?? null"
      [formControl]="formControl"
      [formlyAttributes]="field"
      [nzMode]="props.multiple ? 'multiple' : 'default'"
      nzShowSearch
      (ngModelChange)="props.change && props.change(field, $event)">
      <ng-container *ngFor="let item of props.options | formlySelectOptions: field | async">
        <nz-option-group
          *ngIf="item.group"
          [nzLabel]="item.label">
          <nz-option
            *ngFor="let child of item.group"
            [nzValue]="child.value"
            [nzDisabled]="child.disabled"
            [nzLabel]="child.label">
          </nz-option>
        </nz-option-group>
        <nz-option
          *ngIf="!item.group"
          [nzValue]="item.value"
          [nzDisabled]="item.disabled"
          [nzLabel]="item.label">
        </nz-option>
      </ng-container>
    </nz-select>
  `,
  imports: [CommonModule, ReactiveFormsModule, FormlyModule, FormlySelectModule, NzSelectModule],
})
export class SearchableSelectTypeComponent extends FieldType<FieldTypeConfig<SearchableSelectProps>> {}
