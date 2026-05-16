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

import { NgFor } from "@angular/common";
import { Component } from "@angular/core";
import { ReactiveFormsModule } from "@angular/forms";
import { FieldType, FieldTypeConfig, FormlyModule } from "@ngx-formly/core";
import { NzOptionComponent, NzOptionGroupComponent, NzSelectComponent } from "ng-zorro-antd/select";

interface OpenRouterModelOption {
  value: string;
  label: string;
  company?: string;
}

interface OpenRouterModelGroup {
  company: string;
  options: OpenRouterModelOption[];
}

@Component({
  selector: "texera-openrouter-model-selector",
  template: `
    <nz-select
      nzShowSearch
      nzAllowClear
      [nzPlaceHolder]="props.placeholder ?? 'Select or enter a model'"
      [nzFilterOption]="filterOption"
      [formControl]="formControl"
      [formlyAttributes]="field"
      (nzOnSearch)="onSearch($event)">
      <nz-option-group *ngFor="let group of visibleModelGroups" [nzLabel]="group.company">
        <nz-option *ngFor="let option of group.options" [nzValue]="option.value" [nzLabel]="option.label">
        </nz-option>
      </nz-option-group>
    </nz-select>
  `,
  imports: [FormlyModule, NgFor, NzOptionComponent, NzOptionGroupComponent, NzSelectComponent, ReactiveFormsModule],
})
export class OpenRouterModelSelectorComponent extends FieldType<FieldTypeConfig> {
  searchValue = "";

  get modelOptions(): OpenRouterModelOption[] {
    return Array.isArray(this.props.options) ? this.props.options : [];
  }

  get visibleModelOptions(): OpenRouterModelOption[] {
    const searchValue = this.searchValue.trim();
    if (
      !searchValue ||
      this.modelOptions.some(
        option => option.value === searchValue || option.label.toLowerCase() === searchValue.toLowerCase()
      )
    ) {
      return this.modelOptions;
    }
    return [{ value: searchValue, label: searchValue }, ...this.modelOptions];
  }

  get visibleModelGroups(): OpenRouterModelGroup[] {
    const groups = new Map<string, OpenRouterModelOption[]>();
    this.visibleModelOptions.forEach(option => {
      const company = option.company ?? "";
      groups.set(company, [...(groups.get(company) ?? []), option]);
    });

    return Array.from(groups.entries()).map(([company, options]) => ({ company, options }));
  }

  onSearch(value: string): void {
    this.searchValue = value;
  }

  filterOption = (input: string, option: any): boolean => {
    const searchValue = input.toLowerCase();
    const value = String(option.nzValue ?? option.value ?? "").toLowerCase();
    const label = String(option.nzLabel ?? option.label ?? "").toLowerCase();
    return value.includes(searchValue) || label.includes(searchValue);
  };
}
