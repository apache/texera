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

import { Component, ElementRef, OnDestroy, OnInit, ViewChild, AfterViewInit } from "@angular/core";
import { JupyterPanelService } from "../../service/jupyter-panel/jupyter-panel.service";
import { from, of, Subject } from "rxjs";
import { catchError, switchMap, takeUntil } from "rxjs/operators";
import { DomSanitizer, SafeResourceUrl } from "@angular/platform-browser";
import { NotebookMigrationService } from "../../service/notebook-migration/notebook-migration.service";
import { CommonModule } from "@angular/common";
import { DragDropModule } from "@angular/cdk/drag-drop";
import { NzButtonModule } from "ng-zorro-antd/button";
import { NzIconModule } from "ng-zorro-antd/icon";
import { NzPopconfirmModule } from "ng-zorro-antd/popconfirm";
import { NzTooltipModule } from "ng-zorro-antd/tooltip";

@Component({
  selector: "texera-jupyter-notebook-panel",
  templateUrl: "./jupyter-notebook-panel.component.html",
  styleUrls: ["./jupyter-notebook-panel.component.scss"],
  imports: [CommonModule, DragDropModule, NzButtonModule, NzIconModule, NzPopconfirmModule, NzTooltipModule],
})
export class JupyterNotebookPanelComponent implements OnInit, AfterViewInit, OnDestroy {
  @ViewChild("iframeRef", { static: false }) iframeRef!: ElementRef<HTMLIFrameElement>; // Use static: false

  isVisible: boolean = false; // Initialize to false, meaning the panel is hidden by default
  jupyterUrl: SafeResourceUrl | null = null; // Store the notebook URL dynamically; null when no URL is loaded
  private destroy$ = new Subject<void>();

  constructor(
    private jupyterPanelService: JupyterPanelService,
    private sanitizer: DomSanitizer,
    private notebookMigrationService: NotebookMigrationService
  ) {}

  ngOnInit(): void {
    this.jupyterPanelService.jupyterNotebookPanelVisible$
      .pipe(
        switchMap((visible: boolean) => {
          this.isVisible = visible;

          if (!visible) {
            return of(null);
          }

          return from(this.notebookMigrationService.getJupyterIframeURL()).pipe(
            catchError(() => {
              console.error("Failed to fetch Jupyter iframe URL.");
              return of(null);
            })
          );
        }),
        takeUntil(this.destroy$)
      )
      .subscribe(url => {
        // Always reflect the latest fetch result. A null url (panel hidden, or a
        // failed/empty fetch) clears jupyterUrl so a stale URL is never rendered.
        this.jupyterUrl = url ? this.sanitizer.bypassSecurityTrustResourceUrl(url) : null;
        if (url) {
          this.checkIframeRef();
        }
      });
  }

  ngAfterViewInit(): void {
    // Ensure iframe is handled after it's available in the DOM
    this.checkIframeRef();
  }

  checkIframeRef(): void {
    setTimeout(() => {
      if (!this.isVisible) {
        // Panel hidden; no iframe to register.
        return;
      }
      if (this.iframeRef?.nativeElement) {
        this.jupyterPanelService.setIframeRef(this.iframeRef.nativeElement);
      } else {
        console.error("Jupyter Iframe reference not found.");
      }
    }, 0); // Small timeout to ensure DOM is updated
  }

  ngOnDestroy(): void {
    this.destroy$.next();
    this.destroy$.complete(); // Cleanup subscriptions to avoid memory leaks
  }

  // Delete the workflow's Jupyter notebook by invoking the service method
  deletePanel(): void {
    this.jupyterPanelService.deleteJupyterNotebook();
  }

  // Minimize the jupyter notebook by invoking the service method. Visibility is
  // driven by jupyterNotebookPanelVisible$, so no local isVisible mutation here.
  minimizePanel(): void {
    this.jupyterPanelService.minimizeJupyterNotebookPanel();
  }
}
