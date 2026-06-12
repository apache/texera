/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import { ComponentFixture, TestBed } from "@angular/core/testing";
import { NZ_MODAL_DATA } from "ng-zorro-antd/modal";
import { ConflictingFileModalContentComponent } from "./conflicting-file-modal-content.component";

describe("ConflictingFileModalContentComponent", () => {
  const data = { fileName: "a.csv", path: "/a.csv", size: "1 KB" };
  let component: ConflictingFileModalContentComponent;
  let fixture: ComponentFixture<ConflictingFileModalContentComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [ConflictingFileModalContentComponent],
      providers: [{ provide: NZ_MODAL_DATA, useValue: data }],
    }).compileComponents();
    fixture = TestBed.createComponent(ConflictingFileModalContentComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it("should create", () => {
    expect(component).toBeTruthy();
  });

  it("should expose injected modal data", () => {
    expect(component.data).toEqual(data);
  });
});
