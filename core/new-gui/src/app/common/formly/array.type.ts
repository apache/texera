import { Component } from "@angular/core";
import { FieldArrayType } from "@ngx-formly/core";

@Component({
  template: `
    <ng-container *ngIf="!props.hideLabel">
      <hr>
      <div style="display: flex; justify-content: space-between;">
        <h5>{{ props.label }}</h5>
        <button nz-button [nzSize]="'small'" [nzType]="'primary'" [nzShape]="'circle'" type="button" (click)="add()">
          <span *ngIf="props.addText">{{ props.addText }}</span>
          <i nz-icon nzType="plus" nzTheme="outline"></i>
        </button>
      </div>

      <hr>
    </ng-container>
    <div *ngFor="let field of field.fieldGroup; let i = index;" class="row container__array">
      <formly-field class="col" [field]="field"></formly-field>
      <div class="btn-remove">
        <button nz-button [nzSize]="'small'" [nzShape]="'circle'" nzDanger *ngIf="!props.hideRemoveButtons"
                type="button" (click)="remove(i)">
          <span *ngIf="props.removeText">{{ props.removeText }}</span>
          <i nz-icon nzType="delete" nzTheme="outline"></i>
        </button>
      </div>
    </div>
  `,
})
export class ArrayTypeComponent extends FieldArrayType {}
/**  Copyright 2018 Google Inc. All Rights Reserved.
 Use of this source code is governed by an MIT-style license that
 can be found in the LICENSE file at http://angular.io/license */
