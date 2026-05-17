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

import { Component, inject } from "@angular/core";
import { NZ_MODAL_DATA } from "ng-zorro-antd/modal";
import { DataProfilingPanelComponent } from "./data-profiling-panel.component";

@Component({
  selector: "texera-data-profiling-modal",
  standalone: true,
  imports: [DataProfilingPanelComponent],
  template: `<texera-data-profiling-panel [source]="source"></texera-data-profiling-panel>`,
})
export class DataProfilingModalComponent {
  readonly source: string = inject(NZ_MODAL_DATA, { optional: true })?.source ?? "diabetes.csv";
}
