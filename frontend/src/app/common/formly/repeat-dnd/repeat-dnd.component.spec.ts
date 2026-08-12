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

import { CdkDragDrop, CdkDragHandle } from "@angular/cdk/drag-drop";
import { By } from "@angular/platform-browser";
import { FormArray, FormControl } from "@angular/forms";
import { ComponentFixture, TestBed } from "@angular/core/testing";
import { FormlyRepeatDndComponent } from "./repeat-dnd.component";

describe("FormlyRepeatDndComponent", () => {
  let component: FormlyRepeatDndComponent;
  let fixture: ComponentFixture<FormlyRepeatDndComponent>;

  const createDropEvent = (previousIndex: number, currentIndex: number): CdkDragDrop<string[]> =>
    ({ previousIndex, currentIndex }) as CdkDragDrop<string[]>;

  const setComponentState = (reorder = vi.fn()) => {
    const formControl = new FormArray([new FormControl("a"), new FormControl("b"), new FormControl("c")]);

    component.field = {
      model: ["a", "b", "c"],
      fieldGroup: [{ key: "a" }, { key: "b" }, { key: "c" }],
      formControl,
      props: { reorder },
    } as any;

    return reorder;
  };

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [FormlyRepeatDndComponent],
    }).compileComponents();

    fixture = TestBed.createComponent(FormlyRepeatDndComponent);
    component = fixture.componentInstance;
  });

  it("should create", () => {
    setComponentState();

    expect(component).toBeTruthy();
  });

  it("should do nothing when previousIndex equals currentIndex", () => {
    const reorder = setComponentState();

    component.onDrop(createDropEvent(1, 1));

    expect(component.model).toEqual(["a", "b", "c"]);
    expect(component.field.fieldGroup?.map(field => field.key)).toEqual(["a", "b", "c"]);
    expect((component.formControl as FormArray).controls.map(control => control.value)).toEqual(["a", "b", "c"]);
    expect(reorder).not.toHaveBeenCalled();
  });

  it("should do nothing when model is undefined", () => {
    const reorder = setComponentState();
    component.field = {
      ...component.field,
      model: undefined,
    } as any;

    component.onDrop(createDropEvent(0, 2));

    expect(component.field.fieldGroup?.map(field => field.key)).toEqual(["a", "b", "c"]);
    expect((component.formControl as FormArray).controls.map(control => control.value)).toEqual(["a", "b", "c"]);
    expect(reorder).not.toHaveBeenCalled();
  });

  it("should reorder model, fieldGroup, formControl, and call reorder callback", () => {
    const reorder = setComponentState();

    component.onDrop(createDropEvent(0, 2));

    expect(component.model).toEqual(["b", "c", "a"]);
    expect(component.field.fieldGroup?.map(field => field.key)).toEqual(["b", "c", "a"]);
    expect((component.formControl as FormArray).controls.map(control => control.value)).toEqual(["b", "c", "a"]);
    expect(reorder).toHaveBeenCalledOnce();
  });
  /**
   * The class-level tests above drive onDrop directly and never render. The template owns the rest
   * of the control: one row per entry, which index a row's remove button carries, and whether the
   * section is editable at all.
   */
  describe("rendered rows", () => {
    /** Renders the repeat section with the given template options. */
    function render(templateOptions: Record<string, unknown> = {}): HTMLElement {
      setComponentState();
      component.field = {
        ...component.field,
        fieldGroup: [{ key: "a" }, { key: "b" }, { key: "c" }],
        templateOptions,
      } as any;
      fixture.detectChanges();
      return fixture.nativeElement as HTMLElement;
    }

    function removeButtons(): HTMLButtonElement[] {
      return Array.from(
        (fixture.nativeElement as HTMLElement).querySelectorAll<HTMLButtonElement>(".dnd-remove-button")
      );
    }

    function addButton(): HTMLButtonElement {
      // The add button is the only one outside a row; it carries no class of its own.
      return Array.from((fixture.nativeElement as HTMLElement).querySelectorAll<HTMLButtonElement>("button")).find(
        b => !b.closest(".dnd-row")
      )!;
    }

    it("renders one row per entry", () => {
      const el = render();

      expect(el.querySelectorAll(".dnd-row").length).toBe(3);
    });

    it("gives each row a drag handle", () => {
      // Asserted on the cdkDragHandle directive, not the .drag-handle class: the class is styling
      // and survives the directive being dropped, which would leave the row undraggable.
      render();

      expect(fixture.debugElement.queryAll(By.directive(CdkDragHandle)).length).toBe(3);
    });

    it("removes the row whose button was pressed", () => {
      // The index comes from the ngFor loop variable; a fixed or off-by-one index would delete
      // someone else's row, and every row looks the same on screen.
      const spy = vi.spyOn(component, "remove").mockImplementation(() => {});
      render();

      removeButtons()[1].click();

      expect(spy).toHaveBeenCalledWith(1);
    });

    it("appends a row from the add button", () => {
      const spy = vi.spyOn(component, "add").mockImplementation(() => {});
      render();

      addButton().click();

      expect(spy).toHaveBeenCalledTimes(1);
    });

    it("labels the add button Add when the field does not name it", () => {
      render();

      expect(addButton().textContent?.trim()).toBe("Add");
    });

    it("uses the field's own label for the add button when it has one", () => {
      render({ addText: "Add a column" });

      expect(addButton().textContent?.trim()).toBe("Add a column");
    });

    it("locks the add button for a disabled section", () => {
      // A read-only operator property must not offer edits it cannot persist. nz-button reflects
      // the state as an attribute rather than the DOM property, so it is read that way.
      render({ disabled: true });

      expect(addButton().getAttribute("disabled")).not.toBeNull();
    });

    it("leaves the add button available otherwise", () => {
      render({ disabled: false });

      expect(addButton().getAttribute("disabled")).toBeNull();
    });
  });
});
