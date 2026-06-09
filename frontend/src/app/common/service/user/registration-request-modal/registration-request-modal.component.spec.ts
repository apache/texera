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

import { TestBed } from "@angular/core/testing";
import { NZ_MODAL_DATA } from "ng-zorro-antd/modal";
import { RegistrationRequestModalComponent } from "./registration-request-modal.component";

describe("RegistrationRequestModalComponent", () => {
  beforeEach(() => {
    TestBed.resetTestingModule();
  });

  function setup(data: any): RegistrationRequestModalComponent {
    TestBed.configureTestingModule({
      imports: [RegistrationRequestModalComponent],
      providers: [{ provide: NZ_MODAL_DATA, useValue: data }],
    });
    const fixture = TestBed.createComponent(RegistrationRequestModalComponent);
    return fixture.componentInstance;
  }

  it("should create", () => {
    const component = setup({ uid: 1, email: "a@b.c", name: "Ada" });
    expect(component).toBeTruthy();
  });

  it("copies modal data in the constructor", () => {
    const component = setup({ uid: 1, email: "a@b.c", name: "Ada" });
    expect(component.name).toBe("Ada");
    expect(component.email).toBe("a@b.c");
  });

  it("handles null modal data gracefully", () => {
    const component = setup(null);
    expect(component.name).toBe("");
    expect(component.email).toBe("");
  });

  it("getValues() trims whitespace", () => {
    const component = setup({ uid: 1, email: "a@b.c", name: "Ada" });
    component.affiliation = "  UCI  ";
    component.reason = "  hi ";
    expect(component.getValues()).toEqual({ affiliation: "UCI", reason: "hi" });
  });
});
