import { Component } from "@angular/core";
import { FieldArrayType } from "@ngx-formly/core";

@Component({
  template: `
    <div *ngIf="!props.hideLabel" style="padding-bottom: 10px;">
      <hr>
      <h5 style="display:inline-block;">{{ props.label }}</h5>
      <button nz-button [nzSize]="'small'" [nzType]="'primary'" [nzShape]="'circle'" type="button" (click)="add()" style="display:inline-block;vertical-align: baseline;float: right;">
        <span *ngIf="props.addText">{{ props.addText }}</span>
        <i nz-icon nzType="plus"></i>
      </button>
    </div>
    <div *ngFor="let field of field.fieldGroup; let i = index;" class="row" style="margin: 0;border-top: 1px solid rgba(0,0,0,.1); padding-top: 15px;">
      <formly-field class="col" [field]="field" style="padding-left: 0;"></formly-field>
      <button nz-button [nzSize]="'small'" [nzShape]="'circle'" nzDanger *ngIf="!props.hideRemoveButtons" type="button" (click)="remove(i)">
        <span *ngIf="props.removeText">{{ props.removeText }}</span>
        <i nz-icon nzType="delete"></i>
      </button>
    </div>
  `,
})
export class ArrayTypeComponent extends FieldArrayType {}
/**  Copyright 2018 Google Inc. All Rights Reserved.
 Use of this source code is governed by an MIT-style license that
 can be found in the LICENSE file at http://angular.io/license */
