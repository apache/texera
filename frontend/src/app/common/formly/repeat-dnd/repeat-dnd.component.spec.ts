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

import { CdkDrag, CdkDragDrop, CdkDragHandle, CdkDropList } from "@angular/cdk/drag-drop";
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
    // The callback is the parent's cue to persist, so it has to run AFTER all three reorder
    // steps: a parent that reads the form when notified would otherwise save the pre-drag
    // order and silently discard the drag. Capturing the state from inside the callback is
    // what makes that ordering observable — the final assertions below are order-insensitive,
    // and moveItemInArray mutates in place, so the captures must be copies.
    let seenModel: string[] | undefined;
    let seenFieldKeys: unknown[] | undefined;
    let seenControls: unknown[] | undefined;
    const reorder = setComponentState(
      vi.fn(() => {
        seenModel = [...(component.model as string[])];
        seenFieldKeys = component.field.fieldGroup?.map(field => field.key);
        seenControls = (component.formControl as FormArray).controls.map(control => control.value);
      })
    );

    component.onDrop(createDropEvent(0, 2));

    expect(component.model).toEqual(["b", "c", "a"]);
    expect(component.field.fieldGroup?.map(field => field.key)).toEqual(["b", "c", "a"]);
    expect((component.formControl as FormArray).controls.map(control => control.value)).toEqual(["b", "c", "a"]);
    expect(reorder).toHaveBeenCalledOnce();
    expect(seenModel).toEqual(["b", "c", "a"]);
    expect(seenFieldKeys).toEqual(["b", "c", "a"]);
    expect(seenControls).toEqual(["b", "c", "a"]);
  });

  it("still reorders a section that declares no reorder callback", () => {
    // The reorder callback is how the parent persists the new order, and it is optional:
    // a section rendered without one must still reorder in place rather than throw.
    setComponentState();
    component.field = {
      ...component.field,
      props: {},
    } as any;

    expect(() => component.onDrop(createDropEvent(0, 2))).not.toThrow();
    expect(component.model).toEqual(["b", "c", "a"]);
    expect(component.field.fieldGroup?.map(field => field.key)).toEqual(["b", "c", "a"]);
    expect((component.formControl as FormArray).controls.map(control => control.value)).toEqual(["b", "c", "a"]);
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

    it("makes each row draggable, with its own drag handle", () => {
      // Asserted on the directives, not on the .dnd-row / .drag-handle classes: the classes are
      // styling and survive either directive being dropped. Both are needed — cdkDragHandle
      // constructs happily with no CdkDrag parent (its CDK_DRAG_PARENT injection is optional),
      // so the handle assertion alone passes for a row that cannot be picked up at all, and a
      // row that cannot be picked up never fires cdkDropListDropped.
      render();

      expect(fixture.debugElement.queryAll(By.directive(CdkDrag)).length).toBe(3);
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

    it("leaves the add button available for a section that declares no template options at all", () => {
      // Not `{ disabled: false }`: a schema that says nothing about the repeat section
      // produces no templateOptions object, and an absent object must not read as disabled.
      setComponentState();
      fixture.detectChanges();

      expect(component.field.templateOptions).toBeUndefined();
      expect(addButton().getAttribute("disabled")).toBeNull();
    });

    it("renders each row's own sub-fields", () => {
      setComponentState();
      component.field = {
        ...component.field,
        fieldGroup: [
          { key: "row-0", fieldGroup: [{ key: "row-0-name" }] },
          { key: "row-1", fieldGroup: [{ key: "row-1-name" }] },
        ],
      } as any;
      fixture.detectChanges();

      // Asserted on the config each rendered field was actually handed, not on how many
      // rendered: binding the row itself instead of its sub-field renders the same count
      // of elements and would show up as a pass.
      const rendered = fixture.debugElement.queryAll(By.css("formly-field.dnd-field"));
      expect(rendered.map(f => (f.componentInstance as { field: { key?: unknown } }).field.key)).toEqual([
        "row-0-name",
        "row-1-name",
      ]);
    });

    it("forwards a drop on the row list to onDrop", () => {
      // The drag-and-drop wiring is the whole point of this variant of the repeat section;
      // without the template hookup the rows are draggable but nothing reorders.
      const spy = vi.spyOn(component, "onDrop").mockImplementation(() => {});
      render();
      const event = createDropEvent(0, 2);

      fixture.debugElement.query(By.directive(CdkDropList)).triggerEventHandler("cdkDropListDropped", event);

      expect(spy).toHaveBeenCalledWith(event);
    });
  });
});
