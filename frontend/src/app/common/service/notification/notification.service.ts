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

import { Injectable } from "@angular/core";
import { NzMessageDataOptions, NzMessageService } from "ng-zorro-antd/message";
import { NzNotificationService } from "ng-zorro-antd/notification";

/**
 * NotificationService is an entry service for sending notifications
 */
@Injectable({
  providedIn: "root",
})
export class NotificationService {
  constructor(
    private message: NzMessageService,
    private notification: NzNotificationService
  ) {}

  // Only blank can be removed manually
  blank(title: string, content: string, options: NzMessageDataOptions = {}): void {
    this.notification.blank(title, content, options);
  }

  // Remove current blank notification only
  remove(): void {
    this.notification.remove();
  }

  success(message: string, options: NzMessageDataOptions = {}) {
    this.message.success(message, options);
  }

  info(message: string, options: NzMessageDataOptions = {}) {
    this.message.info(message, options);
  }

  error(message: string, options: NzMessageDataOptions = {}) {
    this.message.error(message, options);
  }

  warning(message: string, options: NzMessageDataOptions = {}) {
    this.message.warning(message, options);
  }

  loading(message: string, options: NzMessageDataOptions = {}) {
    return this.message.loading(message, options);
  }
}
