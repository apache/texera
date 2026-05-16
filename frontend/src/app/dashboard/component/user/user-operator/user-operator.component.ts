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

import { Component, OnInit } from "@angular/core";
import { CommonModule } from "@angular/common";
import { Router, RouterLink } from "@angular/router";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { NzButtonModule } from "ng-zorro-antd/button";
import { NzCardModule } from "ng-zorro-antd/card";
import { NzEmptyModule } from "ng-zorro-antd/empty";
import { NzIconModule } from "ng-zorro-antd/icon";
import { NzPopconfirmModule } from "ng-zorro-antd/popconfirm";
import { NzTagModule } from "ng-zorro-antd/tag";
import { NzTooltipModule } from "ng-zorro-antd/tooltip";
import { CustomOperatorService } from "../../../service/user/custom-operator/custom-operator.service";
import { CustomOperator } from "../../../type/custom-operator.interface";
import { DASHBOARD_USER_OPERATOR_CREATE, DASHBOARD_USER_OPERATOR_EDIT } from "../../../../app-routing.constant";

@UntilDestroy()
@Component({
  selector: "texera-user-operator",
  templateUrl: "./user-operator.component.html",
  styleUrls: ["./user-operator.component.scss"],
  imports: [
    CommonModule,
    RouterLink,
    NzButtonModule,
    NzCardModule,
    NzEmptyModule,
    NzIconModule,
    NzPopconfirmModule,
    NzTagModule,
    NzTooltipModule,
  ],
})
export class UserOperatorComponent implements OnInit {
  public operators: CustomOperator[] = [];

  protected readonly DASHBOARD_USER_OPERATOR_CREATE = DASHBOARD_USER_OPERATOR_CREATE;

  constructor(
    private customOperatorService: CustomOperatorService,
    private router: Router
  ) {}

  ngOnInit(): void {
    this.customOperatorService
      .list$()
      .pipe(untilDestroyed(this))
      .subscribe(ops => (this.operators = ops));
  }

  public openEdit(op: CustomOperator): void {
    this.router.navigate([DASHBOARD_USER_OPERATOR_EDIT, op.id]);
  }

  public delete(op: CustomOperator): void {
    this.customOperatorService.delete(op.id);
  }
}
