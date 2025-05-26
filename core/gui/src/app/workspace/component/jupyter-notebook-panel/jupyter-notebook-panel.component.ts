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
import { Subject } from "rxjs";
import { takeUntil } from "rxjs/operators";

@Component({
  selector: "texera-jupyter-notebook-panel",
  templateUrl: "./jupyter-notebook-panel.component.html",
  styleUrls: ["./jupyter-notebook-panel.component.scss"],
})
export class JupyterNotebookPanelComponent implements OnInit, AfterViewInit, OnDestroy {
  @ViewChild("iframeRef", { static: false }) iframeRef!: ElementRef<HTMLIFrameElement>; // Use static: false

  isVisible: boolean = false; // Initialize to false, meaning the panel is hidden by default
  notebookUrl: string = ""; // Store the notebook URL dynamically
  private destroy$ = new Subject<void>();

  constructor(private jupyterPanelService: JupyterPanelService) {}

  ngOnInit(): void {
    // Subscribe to the visibility state of the panel
    this.jupyterPanelService.jupyterNotebookPanelVisible$
      .pipe(takeUntil(this.destroy$))
      .subscribe((visible: boolean) => {
        this.isVisible = visible;

        if (this.isVisible) {
          // The iframe only exists once the panel is visible (because of *ngIf)
          this.notebookUrl = "http://localhost:8888/notebooks/work/example.ipynb?token=mytoken";
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
      if (this.isVisible && this.iframeRef?.nativeElement) {
        console.log("Iframe reference found:", this.iframeRef.nativeElement);
        this.jupyterPanelService.setIframeRef(this.iframeRef.nativeElement);
      } else {
        console.error("Iframe reference not found yet.");
      }
    }, 0); // Small timeout to ensure DOM is updated
  }

  ngOnDestroy(): void {
    this.destroy$.next();
    this.destroy$.complete(); // Cleanup subscriptions to avoid memory leaks
  }

  // Close the panel by invoking the service method
  closePanel(): void {
    this.jupyterPanelService.closeJupyterNotebookPanel();
  }

  // Minimize the jupyter notebook by invoking the service method
  minimizePanel(): void {
    this.isVisible = false;
    this.jupyterPanelService.minimizeJupyterNotebookPanel();
  }
}
