/**
 * Copyright 2018 Google Inc. All Rights Reserved.
 * Use of this source code is governed by an MIT-style license that
 * can be found in the LICENSE file at http://angular.io/license
 *
 * This file is derived from Angular examples.
 * Source: https://angular.io
 */

import { Component } from "@angular/core";
import { FieldArrayType, FormlyModule } from "@ngx-formly/core";
import { NgFor } from "@angular/common";
import { NzSpaceCompactItemDirective } from "ng-zorro-antd/space";
import { NzButtonComponent } from "ng-zorro-antd/button";
import { NzWaveDirective } from "ng-zorro-antd/core/wave";
import { ɵNzTransitionPatchDirective } from "ng-zorro-antd/core/transition-patch";
import { NzIconDirective } from "ng-zorro-antd/icon";

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
  imports: [
    NgFor,
    FormlyModule,
    NzSpaceCompactItemDirective,
    NzButtonComponent,
    NzWaveDirective,
    ɵNzTransitionPatchDirective,
    NzIconDirective,
  ],
})
export class ArrayTypeComponent extends FieldArrayType {}
