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
 */

import { Component, OnInit } from "@angular/core";
import { CommonModule } from "@angular/common";
import { FormsModule } from "@angular/forms";
import { Router, RouterLink } from "@angular/router";
import { NzInputModule } from "ng-zorro-antd/input";
import { NzButtonModule } from "ng-zorro-antd/button";
import { NzIconModule } from "ng-zorro-antd/icon";
import { NzSelectModule } from "ng-zorro-antd/select";
import { NzTagModule } from "ng-zorro-antd/tag";
import { NzTooltipModule } from "ng-zorro-antd/tooltip";
import { NzModalService } from "ng-zorro-antd/modal";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { WorkflowHubService } from "../workflow-hub.service";
import {
  WorkflowHubEntry,
  WorkflowHubSort,
  WORKFLOW_HUB_CATEGORIES,
  WORKFLOW_HUB_SORTS,
  WorkflowHubCategory,
} from "../workflow-hub.types";
import { WorkflowHubPublishDialogComponent } from "../workflow-hub-publish-dialog/workflow-hub-publish-dialog.component";

@UntilDestroy()
@Component({
  selector: "texera-workflow-hub-list",
  templateUrl: "./workflow-hub-list.component.html",
  styleUrls: ["./workflow-hub-list.component.scss"],
  imports: [
    CommonModule,
    FormsModule,
    RouterLink,
    NzInputModule,
    NzButtonModule,
    NzIconModule,
    NzSelectModule,
    NzTagModule,
    NzTooltipModule,
  ],
})
export class WorkflowHubListComponent implements OnInit {
  readonly categories = WORKFLOW_HUB_CATEGORIES;
  readonly sortOptions = WORKFLOW_HUB_SORTS;

  search = "";
  activeCategory: WorkflowHubCategory | "all" = "all";
  sort: WorkflowHubSort = "trending";

  entries: WorkflowHubEntry[] = [];
  starred = new Set<string>();

  constructor(
    private hubService: WorkflowHubService,
    private router: Router,
    private modal: NzModalService
  ) {}

  ngOnInit(): void {
    this.hubService
      .entries$()
      .pipe(untilDestroyed(this))
      .subscribe(entries => (this.entries = entries));
    this.hubService
      .stars$()
      .pipe(untilDestroyed(this))
      .subscribe(set => (this.starred = set));
  }

  get filtered(): WorkflowHubEntry[] {
    const q = this.search.trim().toLowerCase();
    let list = this.entries.filter(e => {
      if (this.activeCategory !== "all" && e.category !== this.activeCategory) return false;
      if (!q) return true;
      return (
        e.title.toLowerCase().includes(q) ||
        e.description.toLowerCase().includes(q) ||
        e.tags.some(t => t.toLowerCase().includes(q)) ||
        e.authorName.toLowerCase().includes(q)
      );
    });
    switch (this.sort) {
      case "stars":
        list = [...list].sort((a, b) => b.stars - a.stars);
        break;
      case "forks":
        list = [...list].sort((a, b) => b.forks - a.forks);
        break;
      case "recent":
        list = [...list].sort((a, b) => Date.parse(b.publishedAt) - Date.parse(a.publishedAt));
        break;
      case "trending":
      default:
        list = [...list].sort(
          (a, b) =>
            this.trendingScore(b) - this.trendingScore(a)
        );
    }
    return list;
  }

  get featured(): WorkflowHubEntry[] {
    if (this.activeCategory !== "all" || this.search.trim().length > 0) return [];
    return this.entries.filter(e => e.featured).slice(0, 3);
  }

  get nonFeatured(): WorkflowHubEntry[] {
    if (this.featured.length === 0) return this.filtered;
    const featuredIds = new Set(this.featured.map(e => e.id));
    return this.filtered.filter(e => !featuredIds.has(e.id));
  }

  get activeCategoryLabel(): string {
    if (this.activeCategory === "all") return "All Workflows";
    return this.categories.find(c => c.key === this.activeCategory)?.label ?? "Workflows";
  }

  private trendingScore(e: WorkflowHubEntry): number {
    // simple recency-weighted popularity
    const ageDays = Math.max(1, (Date.now() - Date.parse(e.publishedAt)) / 86400000);
    return (e.stars * 2 + e.forks * 3 + e.views * 0.1) / Math.log2(ageDays + 2);
  }

  selectCategory(key: WorkflowHubCategory | "all"): void {
    this.activeCategory = key;
  }

  toggleStar(entry: WorkflowHubEntry, event: Event): void {
    event.stopPropagation();
    event.preventDefault();
    this.hubService.toggleStar(entry.id);
  }

  openDetail(entry: WorkflowHubEntry): void {
    this.router.navigate(["/dashboard/hub/workflow-hub/detail", entry.id]);
  }

  openPublishDialog(): void {
    this.modal.create({
      nzTitle: "Publish to Workflow Hub",
      nzContent: WorkflowHubPublishDialogComponent,
      nzFooter: null,
      nzWidth: 640,
      nzMaskClosable: false,
    });
  }

  trackById(_: number, e: WorkflowHubEntry): string {
    return e.id;
  }
}
