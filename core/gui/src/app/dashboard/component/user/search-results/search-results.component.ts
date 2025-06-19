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

import { Component, EventEmitter, Input, OnInit, Output } from "@angular/core";
import { DashboardEntry } from "../../../type/dashboard-entry";
import { UserService } from "../../../../common/service/user/user.service";
import { finalize, Observable, throwError } from "rxjs";
import { map, tap } from "rxjs/operators";

export type LoadMoreFunction = (
  start: number,
  count: number
) => Observable<{ entries: DashboardEntry[]; more: boolean }>;

@Component({
  selector: "texera-search-results",
  templateUrl: "./search-results.component.html",
  styleUrls: ["./search-results.component.scss"],
})
export class SearchResultsComponent {
  loadMoreFunction: LoadMoreFunction | null = null;
  loading = false;
  more = false;
  entries: ReadonlyArray<DashboardEntry> = [];
  private resetCounter = 0;
  @Input() isPrivateSearch = false;
  @Input() showResourceTypes = false;
  @Input() public pid: number = 0;
  @Input() editable = false;
  @Input() searchKeywords: string[] = [];
  @Input() currentUid: number | undefined;
  @Output() deleted = new EventEmitter<DashboardEntry>();
  @Output() duplicated = new EventEmitter<DashboardEntry>();
  @Output() modified = new EventEmitter<DashboardEntry>();
  @Output() notifyWorkflow = new EventEmitter<void>();
  @Output() refresh = new EventEmitter<void>();

  constructor(private userService: UserService) {}

  getUid(): number | undefined {
    return this.userService.getCurrentUser()?.uid;
  }

  reset(loadMoreFunction: LoadMoreFunction): void {
    this.entries = [];
    this.loadMoreFunction = loadMoreFunction;
    this.resetCounter++;
  }

  public loadMore(): Observable<void> {
    if (!this.loadMoreFunction) {
      return throwError(() => new Error("This is an empty list and cannot load more entries."));
    }
    this.loading = true;
    const originalResetCounter = this.resetCounter;

    return this.loadMoreFunction(this.entries.length, 20).pipe(
      tap(results => {
        if (this.resetCounter !== originalResetCounter) {
          return;
        }
        this.entries = [...this.entries, ...results.entries];
        this.more = results.more;
      }),
      finalize(() => {
        this.loading = false;
      }),
      map(() => {})
    );
  }

  onEntryCheckboxChange(): void {
    const allSelected = this.entries.every(entry => entry.checked);
    if (allSelected) {
      this.notifyWorkflow.emit();
    }
  }

  selectAll(): void {
    this.entries.forEach(entry => (entry.checked = true));
  }

  clearAllSelections() {
    this.entries.forEach(entry => (entry.checked = false));
  }
}
