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
import { FormControl } from "@angular/forms";
import { FieldTypeConfig } from "@ngx-formly/core";
import { of } from "rxjs";
import { NzModalService } from "ng-zorro-antd/modal";
import { DatasetVersionSelectorComponent } from "./dataset-version-selector.component";

describe("DatasetVersionSelectorComponent", () => {
  let component: DatasetVersionSelectorComponent;
  let fixture: ComponentFixture<DatasetVersionSelectorComponent>;
  // The component's only dependency is NzModalService, and it only reads
  // `afterClose` off the returned modal ref. Each test overrides what
  // `afterClose` emits via `mockReturnValue`.
  let modalServiceSpy: { create: ReturnType<typeof vi.fn> };

  // Attach a fresh FormControl with a known starting value, since the component
  // extends FieldType and reads/writes through `this.formControl`.
  function setFormControl(initialValue: string): FormControl {
    const formControl = new FormControl(initialValue);
    component.field = { formControl } as FieldTypeConfig;
    return formControl;
  }

  beforeEach(async () => {
    modalServiceSpy = { create: vi.fn() };

    await TestBed.configureTestingModule({
      imports: [DatasetVersionSelectorComponent],
      providers: [{ provide: NzModalService, useValue: modalServiceSpy }],
    }).compileComponents();

    fixture = TestBed.createComponent(DatasetVersionSelectorComponent);
    component = fixture.componentInstance;
  });

  it("should create", () => {
    expect(component).toBeTruthy();
  });

  it("opens the dataset selection modal once when clicked", () => {
    modalServiceSpy.create.mockReturnValue({ afterClose: of(undefined) });
    setFormControl("");

    component.onClickOpenDatasetSelectionModal();

    expect(modalServiceSpy.create).toHaveBeenCalledTimes(1);
  });

  it("writes the chosen path back into the form control when a version is selected", () => {
    const formControl = setFormControl("");
    modalServiceSpy.create.mockReturnValue({ afterClose: of("/dataset/v1") });

    component.onClickOpenDatasetSelectionModal();

    expect(formControl.value).toBe("/dataset/v1");
  });

  it("leaves the form control unchanged when the modal is dismissed without a value", () => {
    const formControl = setFormControl("/existing/v2");
    modalServiceSpy.create.mockReturnValue({ afterClose: of("") });

    component.onClickOpenDatasetSelectionModal();

    expect(formControl.value).toBe("/existing/v2");
  });

  /**
   * The tests above call onClickOpenDatasetSelectionModal directly, so the
   * template never rendered: the read-only box that shows the current selection
   * and the button that opens the picker were both unexercised.
   */
  describe("rendered field", () => {
    const selectionBox = (): HTMLInputElement | null => fixture.nativeElement.querySelector("input[nz-input]");
    const openButton = (): HTMLButtonElement => fixture.nativeElement.querySelector("button[nz-button]");

    it("shows nothing but the picker button until a dataset has been chosen", () => {
      setFormControl("");
      fixture.detectChanges();

      expect(selectionBox()).toBeNull();
      expect(openButton().textContent!.trim()).toBe("Select Dataset");
    });

    it("shows the chosen path in a read-only box", () => {
      setFormControl("/dataset/owner@x.com/myds/v1");
      fixture.detectChanges();

      const box = selectionBox()!;
      expect(box.value).toBe("/dataset/owner@x.com/myds/v1");
      // The path is only editable through the picker, never by typing.
      expect(box.readOnly).toBe(true);
      expect(box.required).toBe(true);
    });

    it("opens the picker from the button and shows whatever it returns", () => {
      setFormControl("");
      fixture.detectChanges();
      modalServiceSpy.create.mockReturnValue({ afterClose: of("/dataset/owner@x.com/myds/v2") });

      openButton().click();
      fixture.detectChanges();

      expect(modalServiceSpy.create).toHaveBeenCalledTimes(1);
      expect(selectionBox()!.value).toBe("/dataset/owner@x.com/myds/v2");
    });
  });
});
