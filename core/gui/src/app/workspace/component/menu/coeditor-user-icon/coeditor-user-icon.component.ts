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

import { Component, Input } from "@angular/core";
import { Coeditor, Role } from "../../../../common/type/user";
import { CoeditorPresenceService } from "../../../service/workflow-graph/model/coeditor-presence.service";

/**
 * CoeditorUserIconComponent is the user icon of a co-editor.
 *
 * It is also the entry for shadowing mode.
 */

@Component({
  selector: "texera-coeditor-user-icon",
  templateUrl: "coeditor-user-icon.component.html",
  styleUrls: ["coeditor-user-icon.component.css"],
})
export class CoeditorUserIconComponent {
  @Input() coeditor: Coeditor = { name: "", email: "", uid: -1, role: Role.REGULAR, comment: "", clientId: "0" };

  constructor(public coeditorPresenceService: CoeditorPresenceService) {}

  public shadowCoeditor() {
    this.coeditorPresenceService.shadowCoeditor(this.coeditor);
  }

  stopShadowing() {
    this.coeditorPresenceService.stopShadowing();
  }
}
