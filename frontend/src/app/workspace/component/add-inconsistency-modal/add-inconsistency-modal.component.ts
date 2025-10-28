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
import { NzModalRef } from "ng-zorro-antd/modal";
import { DataInconsistencyService } from "../../service/data-inconsistency/data-inconsistency.service";

@Component({
  selector: "texera-add-inconsistency-modal",
  templateUrl: "./add-inconsistency-modal.component.html",
  styleUrls: ["./add-inconsistency-modal.component.scss"],
})
export class AddInconsistencyModalComponent implements OnInit {
  name: string = "";
  description: string = "";
  operatorId: string = "";

  constructor(
    private modalRef: NzModalRef,
    private dataInconsistencyService: DataInconsistencyService
  ) {}

  ngOnInit(): void {
    // Get the operatorId passed from the modal
    const data = this.modalRef.getConfig().nzData;
    if (data && data.operatorId) {
      this.operatorId = data.operatorId;
    }
  }

  onSubmit(): void {
    if (this.name.trim() && this.description.trim()) {
      this.dataInconsistencyService.addInconsistency(this.name.trim(), this.description.trim(), this.operatorId);
      this.modalRef.close();
    }
  }

  onCancel(): void {
    this.modalRef.close();
  }
}
