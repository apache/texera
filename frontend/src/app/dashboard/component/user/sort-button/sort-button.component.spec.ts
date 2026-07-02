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

import { ComponentFixture, TestBed } from "@angular/core/testing";
import { SortButtonComponent } from "./sort-button.component";
import { SortMethod } from "../../../type/sort-method";

describe("SortButtonComponent", () => {
  let component: SortButtonComponent;
  let fixture: ComponentFixture<SortButtonComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [SortButtonComponent],
    }).compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(SortButtonComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it("should create the component with correct default sorting method", () => {
    expect(component).toBeTruthy();
    expect(component.sortMethod).toBe(SortMethod.EditTimeDesc);
  });

  it("should handle lastSort() correctly", () => {
    const emitSpy = vi.spyOn(component.sortMethodChange, "emit");
    component.lastSort();
    expect(component.sortMethod).toBe(SortMethod.EditTimeDesc);
    expect(emitSpy).toHaveBeenCalledWith(SortMethod.EditTimeDesc);
  });

  it("should handle dateSort() correctly", () => {
    const emitSpy = vi.spyOn(component.sortMethodChange, "emit");
    component.dateSort();
    expect(component.sortMethod).toBe(SortMethod.CreateTimeDesc);
    expect(emitSpy).toHaveBeenCalledWith(SortMethod.CreateTimeDesc);
  });

  it("should handle ascSort() correctly", () => {
    const emitSpy = vi.spyOn(component.sortMethodChange, "emit");
    component.ascSort();
    expect(component.sortMethod).toBe(SortMethod.NameAsc);
    expect(emitSpy).toHaveBeenCalledWith(SortMethod.NameAsc);
  });

  it("should handle dscSort() correctly", () => {
    const emitSpy = vi.spyOn(component.sortMethodChange, "emit");
    component.dscSort();
    expect(component.sortMethod).toBe(SortMethod.NameDesc);
    expect(emitSpy).toHaveBeenCalledWith(SortMethod.NameDesc);
  });

  it("should handle execSort() correctly", () => {
    const emitSpy = vi.spyOn(component.sortMethodChange, "emit");
    component.execSort();
    expect(component.sortMethod).toBe(SortMethod.ExecutionTimeDesc);
    expect(emitSpy).toHaveBeenCalledWith(SortMethod.ExecutionTimeDesc);
  });
});
