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
