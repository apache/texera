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

import { Component, OnInit, OnDestroy } from "@angular/core";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import {
  DataInconsistencyService,
  DataInconsistency,
} from "../../../service/data-inconsistency/data-inconsistency.service";

@UntilDestroy()
@Component({
  selector: "texera-inconsistency-list",
  templateUrl: "./inconsistency-list.component.html",
  styleUrls: ["./inconsistency-list.component.scss"],
})
export class InconsistencyListComponent implements OnInit, OnDestroy {
  inconsistencies: DataInconsistency[] = [];

  constructor(private inconsistencyService: DataInconsistencyService) {}

  ngOnInit(): void {
    // Subscribe to inconsistency updates
    this.inconsistencyService
      .getInconsistencies()
      .pipe(untilDestroyed(this))
      .subscribe(inconsistencies => {
        this.inconsistencies = inconsistencies;
      });
  }

  ngOnDestroy(): void {
    // Cleanup handled by @UntilDestroy
  }

  /**
   * Delete an inconsistency
   */
  deleteInconsistency(id: string): void {
    this.inconsistencyService.deleteInconsistency(id);
  }

  /**
   * Clear all inconsistencies
   */
  clearAll(): void {
    this.inconsistencyService.clearAll();
  }
}
