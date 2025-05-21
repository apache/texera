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

import { Component, Input, OnChanges } from "@angular/core";
import { UserService } from "../../../../common/service/user/user.service";
import { Observable, of } from "rxjs";
@Component({
  selector: "texera-user-avatar",
  templateUrl: "./user-avatar.component.html",
  styleUrls: ["./user-avatar.component.scss"],
})

/**
 * UserAvatarComponent is used to show the avatar of a user
 * The avatar of a Google user will be its Google profile picture
 * The avatar of a normal user will be a default one with the initial
 */
export class UserAvatarComponent implements OnChanges {
  @Input() googleAvatar?: string;
  @Input() userName?: string;
  @Input() userColor?: string;
  @Input() isOwner: Boolean = false;
  avatarUrl$: Observable<string | undefined> = of(undefined);

  constructor(private userService: UserService) {}

  ngOnChanges(): void {
    if (this.googleAvatar) {
      this.avatarUrl$ = this.userService.getAvatar(this.googleAvatar);
    } else {
      this.avatarUrl$ = of(undefined);
    }
  }

  /**
   * abbreviates the name under 5 chars
   * @param userName
   */
  public abbreviate(userName: string): string {
    if (userName.length <= 5) {
      return userName;
    } else {
      return userName.slice(0, 5);
    }
  }
}
