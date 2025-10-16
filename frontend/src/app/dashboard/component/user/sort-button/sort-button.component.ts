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

import { Component, EventEmitter, Output } from "@angular/core";
import { SortMethod } from "../../../type/sort-method";

@Component({
  selector: "texera-sort-button",
  templateUrl: "./sort-button.component.html",
  styleUrls: ["./sort-button.component.scss"],
})
export class SortButtonComponent {
  @Output()
  public sortMethodChange = new EventEmitter<SortMethod>();
  public sortMethod = SortMethod.EditTimeDesc;

  public lastSort(): void {
    this.sortMethod = SortMethod.EditTimeDesc;
    this.sortMethodChange.emit(this.sortMethod);
  }

  public dateSort(): void {
    this.sortMethod = SortMethod.CreateTimeDesc;
    this.sortMethodChange.emit(this.sortMethod);
  }

  public ascSort(): void {
    this.sortMethod = SortMethod.NameAsc;
    this.sortMethodChange.emit(this.sortMethod);
  }

  public dscSort(): void {
    this.sortMethod = SortMethod.NameDesc;
    this.sortMethodChange.emit(this.sortMethod);
  }
}
