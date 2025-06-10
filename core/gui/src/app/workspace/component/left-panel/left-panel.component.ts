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

import { AfterViewInit, Component, ElementRef, HostListener, OnDestroy, OnInit, Type, ViewChild } from "@angular/core";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { NzResizeEvent } from "ng-zorro-antd/resizable";
import { CdkDragDrop, moveItemInArray } from "@angular/cdk/drag-drop";
import { OperatorMenuComponent } from "./operator-menu/operator-menu.component";
import { VersionsListComponent } from "./versions-list/versions-list.component";
import { WorkflowExecutionHistoryComponent } from "../../../dashboard/component/user/user-workflow/ngbd-modal-workflow-executions/workflow-execution-history.component";
import { TimeTravelComponent } from "./time-travel/time-travel.component";
import { SettingsComponent } from "./settings/settings.component";
import { calculateTotalTranslate3d } from "../../../common/util/panel-dock";
import { PanelService } from "../../service/panel/panel.service";
import { GuiConfigService } from "../../../common/service/gui-config.service";
@UntilDestroy()
@Component({
  selector: "texera-left-panel",
  templateUrl: "left-panel.component.html",
  styleUrls: ["left-panel.component.scss"],
})
export class LeftPanelComponent implements OnDestroy, OnInit, AfterViewInit {
  @ViewChild("content") content!: ElementRef<HTMLDivElement>;
  protected readonly window = window;
  private static readonly MIN_PANEL_WIDTH = 230;
  currentComponent: Type<any> | null = null;
  title = "Operators";
  width = LeftPanelComponent.MIN_PANEL_WIDTH;
  minPanelHeight = 410;
  height = Math.max(this.minPanelHeight, window.innerHeight * 0.6);
  id = -1;
  currentIndex = 0;
  items = [
    { component: null, title: "", icon: "", enabled: true },
    { component: OperatorMenuComponent, title: "Operators", icon: "appstore", enabled: true },
    { component: VersionsListComponent, title: "Versions", icon: "schedule", enabled: false },
    {
      component: SettingsComponent,
      title: "Settings",
      icon: "setting",
      enabled: true,
    },
    {
      component: WorkflowExecutionHistoryComponent,
      title: "Execution History",
      icon: "history",
      enabled: false,
    },
    {
      component: TimeTravelComponent,
      title: "Time Travel",
      icon: "clock-circle",
      enabled: false,
    },
  ];

  order = Array.from({ length: this.items.length - 1 }, (_, index) => index + 1);
  dragPosition = { x: 0, y: 0 };
  returnPosition = { x: 0, y: 0 };
  isDocked = true;

  constructor(
    private panelService: PanelService,
    private config: GuiConfigService
  ) {
    // Initialize items array with config values
    this.updateItemsWithConfig();

    const savedOrder = localStorage.getItem("left-panel-order")?.split(",").map(Number);
    this.order = savedOrder && new Set(savedOrder).size === new Set(this.order).size ? savedOrder : this.order;

    const savedIndex = Number(localStorage.getItem("left-panel-index"));
    this.openFrame(savedIndex < this.items.length && this.items[savedIndex].enabled ? savedIndex : 1);

    this.width = Number(localStorage.getItem("left-panel-width")) || this.width;
    this.height = Number(localStorage.getItem("left-panel-height")) || this.height;
  }

  private updateItemsWithConfig(): void {
    this.items[2].enabled = this.config.env.userSystemEnabled; // Versions
    this.items[4].enabled = this.config.env.workflowExecutionsTrackingEnabled; // Execution History
    this.items[5].enabled = this.config.env.userSystemEnabled && this.config.env.timetravelEnabled; // Time Travel
  }

  ngOnInit(): void {
    const style = localStorage.getItem("left-panel-style");
    if (style) document.getElementById("left-container")!.style.cssText = style;
    const translates = document.getElementById("left-container")!.style.transform;
    const [xOffset, yOffset, _] = calculateTotalTranslate3d(translates);
    this.returnPosition = { x: -xOffset, y: -yOffset };
    this.isDocked = this.dragPosition.x === this.returnPosition.x && this.dragPosition.y === this.returnPosition.y;
    this.panelService.closePanelStream.pipe(untilDestroyed(this)).subscribe(() => this.openFrame(0));
    this.panelService.resetPanelStream.pipe(untilDestroyed(this)).subscribe(() => {
      this.resetPanelPosition();
      this.openFrame(1);
    });
  }

  // Calculates the sum of level one operator tabs, and sets minPanelHeight to this value
  ngAfterViewInit(): void {
    setTimeout(() => {
      const topLevelCategories = this.content.nativeElement.querySelectorAll(
        "nz-collapse-panel.operator-group[data-depth=\"0\"]"
      );

      if (topLevelCategories.length > 0) {
        let totalCategoriesHeight = 0;
        topLevelCategories.forEach(element => {
          totalCategoriesHeight += element.clientHeight;
        });

        let padding = 90;
        this.minPanelHeight = totalCategoriesHeight + padding; // Add padding for search bar and other UI elements
        this.height = this.minPanelHeight;
      }
    }, 0); // Wait for collapsible panels to render
  }

  @HostListener("window:beforeunload")
  ngOnDestroy(): void {
    localStorage.setItem("left-panel-width", String(this.width));
    localStorage.setItem("left-panel-height", String(this.height));
    localStorage.setItem("left-panel-order", String(this.order));
    localStorage.setItem("left-panel-index", String(this.currentIndex));

    const leftContainer = document.getElementById("left-container");
    if (leftContainer) {
      localStorage.setItem("left-panel-style", leftContainer.style.cssText);
    }
  }

  openFrame(i: number) {
    if (!i) {
      this.width = 0;
      this.height = 65;
    } else if (!this.width) {
      this.width = LeftPanelComponent.MIN_PANEL_WIDTH;
      this.height = this.minPanelHeight;
    }
    this.title = this.items[i].title;
    this.currentComponent = this.items[i].component;
    this.currentIndex = i;
  }
  onDrop(event: CdkDragDrop<string[]>) {
    moveItemInArray(this.order, event.previousIndex, event.currentIndex);
  }
  onResize({ width, height }: NzResizeEvent) {
    cancelAnimationFrame(this.id);
    this.id = requestAnimationFrame(() => {
      this.width = width!;
      this.height = height!;
    });
  }

  resetPanelPosition() {
    this.dragPosition = { x: this.returnPosition.x, y: this.returnPosition.y };
    this.isDocked = true;
  }

  handleDragStart() {
    this.isDocked = false;
  }
}
