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

import { Component, EventEmitter, Input, OnChanges, Output } from "@angular/core";
import { PermissionTemplate } from "../../../../service/admin/user/admin-user.service";
import { User } from "../../../../../common/type/user";

@Component({
  selector: "texera-permission-edit-modal",
  templateUrl: "./permission-edit-modal.component.html",
  styleUrls: ["./permission-edit-modal.component.scss"],
})
export class PermissionEditModalComponent implements OnChanges {
  @Input() isVisible: boolean = false;
  @Input() permissionTemplate: PermissionTemplate | null = null;
  @Input() user: User | null = null;
  @Output() save = new EventEmitter<string>();
  @Output() cancel = new EventEmitter<void>();

  editPermission: string = "{}";
  permissionObject: any = {};
  modalTitle: string = "Edit Permissions";

  ngOnChanges(): void {
    if (this.user) {
      this.modalTitle = `Edit Permissions for ${this.user.name}`;
      this.editPermission = this.user.permission || "{}";
      this.permissionObject = this.parsePermission(this.editPermission);
    }
  }

  parsePermission(permission: string): any {
    try {
      return JSON.parse(permission);
    } catch {
      return {};
    }
  }

  getPermissionKeys(): string[] {
    return this.permissionTemplate ? Object.keys(this.permissionTemplate.permissions) : [];
  }

  updatePermissionValue(permissionKey: string, value: any): void {
    this.permissionObject[permissionKey] = value;
    this.editPermission = JSON.stringify(this.permissionObject);
  }

  handleCancel(): void {
    this.cancel.emit();
  }

  handleSave(): void {
    this.save.emit(this.editPermission);
  }
}
