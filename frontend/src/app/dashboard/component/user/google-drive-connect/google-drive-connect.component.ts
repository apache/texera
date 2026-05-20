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
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { DriveService, DriveFolder } from "../../../service/user/google-drive/drive.service";
import { NgIf } from "@angular/common";
import { NzButtonComponent } from "ng-zorro-antd/button";
import { NzIconDirective } from "ng-zorro-antd/icon";

@UntilDestroy()
@Component({
  selector: "texera-google-drive-connect",
  template: `
    <div>
      <p>Google Drive Status: {{ status }}</p>
      <button nz-button nzType="primary" (click)="connect()" [disabled]="status === 'connected'">
        <span nz-icon nzType="google" nzTheme="outline"></span>
        Connect Google Drive
      </button>
      <button nz-button nzDanger style="margin-left: 8px" (click)="reconnect()" *ngIf="status === 'connected'">
        Reconnect
      </button>
      <button nz-button nzType="default" style="margin-left: 8px" (click)="openFolderPicker()" *ngIf="status === 'connected'">
        <span nz-icon nzType="folder-open" nzTheme="outline"></span>
        Choose Export Folder
      </button>
      <p *ngIf="selectedFolder">Export to: {{ selectedFolder.name }}</p>
    </div>
  `,
  imports: [NgIf, NzButtonComponent, NzIconDirective],
})
export class GoogleDriveConnectComponent implements OnInit {
  status = "checking...";
  selectedFolder: DriveFolder | null = null;

  constructor(private driveService: DriveService) {}

  ngOnInit(): void {
    this.driveService
      .getToken()
      .pipe(untilDestroyed(this))
      .subscribe({
        next: res => {
          this.status = res.status === "ok" ? "connected" : res.status;
        },
        error: () => {
          this.status = "error";
        },
      });

    this.driveService
      .onConnected()
      .pipe(untilDestroyed(this))
      .subscribe(() => {
        this.status = "connected";
      });
  }

  connect(): void {
    this.driveService.connect();
  }

  reconnect(): void {
    this.driveService.connect(true);
  }

  openFolderPicker(): void {
    this.driveService
      .openFolderPicker()
      .pipe(untilDestroyed(this))
      .subscribe(folder => {
        this.selectedFolder = folder;
      });
  }
}
