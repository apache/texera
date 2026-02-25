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

import { ChangeDetectionStrategy, Component, inject } from "@angular/core";
import { NZ_MODAL_DATA } from "ng-zorro-antd/modal";

export interface ConflictingFileModalData {
  fileName: string;
  path: string;
  size: string;
}

@Component({
  selector: "texera-conflicting-file-modal-content",
  templateUrl: "./conflicting-file-modal-content.component.html",
  styleUrls: ["./conflicting-file-modal-content.component.scss"],
  changeDetection: ChangeDetectionStrategy.OnPush,
})
export class ConflictingFileModalContentComponent {
  readonly data: ConflictingFileModalData = inject(NZ_MODAL_DATA);
}
