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

import { CommonModule } from "@angular/common";
import { ComponentFixture, TestBed } from "@angular/core/testing";
import { FormsModule } from "@angular/forms";
import { BreakpointConditionInputComponent } from "./breakpoint-condition-input.component";
import { UdfDebugService } from "../../../service/operator-debug/udf-debug.service";
import { SimpleChanges } from "@angular/core";
import { commonTestProviders } from "../../../../common/testing/test-utils";
import type { Mocked } from "vitest";
import type { editor } from "monaco-editor";
describe("BreakpointConditionInputComponent", () => {
  let component: BreakpointConditionInputComponent;
  let fixture: ComponentFixture<BreakpointConditionInputComponent>;
  let mockUdfDebugService: Mocked<UdfDebugService>;

  beforeEach(async () => {
    // Create a mock UdfDebugService
    mockUdfDebugService = {
      getCondition: vi.fn(),
      doUpdateBreakpointCondition: vi.fn(),
    } as unknown as Mocked<UdfDebugService>;

    await TestBed.configureTestingModule({
      imports: [BreakpointConditionInputComponent, CommonModule, FormsModule],
      providers: [{ provide: UdfDebugService, useValue: mockUdfDebugService }, ...commonTestProviders],
    }).compileComponents();

    fixture = TestBed.createComponent(BreakpointConditionInputComponent);
    component = fixture.componentInstance;

    component.monacoEditor = {
      getLayoutInfo: () => ({ glyphMarginLeft: 10 }),
      getDomNode: () =>
        ({
          getBoundingClientRect: () => ({ top: 20, left: 30 }),
        }) as HTMLDivElement,
      getBottomForLineNumber: () => 40,
      getScrollTop: () => 5,
      getScrollLeft: () => 0,
      dispose: vi.fn(),
    } as unknown as editor.IStandaloneCodeEditor;

    // Set required inputs
    component.operatorId = "test-operator";
    component.lineNum = 1;

    fixture.detectChanges(); // Trigger Angular's change detection
  });

  afterEach(() => {
    // Clean up the editor and DOM element after each test
    component.monacoEditor.dispose();
    component.closeEmitter.emit();
  });

  /**
   * The popup has no layout of its own: it is positioned by arithmetic over the Monaco editor's
   * reported geometry. The stub above supplies distinguishable non-zero values
   * (glyphMarginLeft 10, editor rect top 20 / left 30, line bottom 40, scrollTop 5), so every
   * expectation below is a specific number rather than a zero that jsdom would produce anyway.
   */
  describe("popup placement", () => {
    it("offsets left by the glyph margin and the fixed popup width", () => {
      // 30 (rect.left) + 10 (glyphMarginLeft) - 0 (scrollLeft) - 160 (popup width)
      expect(component.left()).toBe(-120);
    });

    it("places the top at the bottom of the target line, less the scroll offset", () => {
      // 20 (rect.top) + 40 (line bottom) - 5 (scrollTop)
      expect(component.top()).toBe(55);
    });

    it("subtracts the horizontal scroll offset", () => {
      // Only left() reads scrollLeft; a copy-paste of top()'s body would miss this.
      component.monacoEditor = {
        ...component.monacoEditor,
        getScrollLeft: () => 25,
      } as unknown as editor.IStandaloneCodeEditor;

      expect(component.left()).toBe(-145);
    });

    it("falls back to 0 rather than throwing when there is no editor yet", () => {
      const stub = component.monacoEditor;
      component.monacoEditor = undefined as unknown as editor.IStandaloneCodeEditor;
      try {
        // The guards exist because the popup can be rendered a tick before the editor is attached.
        expect(component.left()).toBe(0);
        expect(component.top()).toBe(0);
      } finally {
        // afterEach disposes the editor, so put the stub back.
        component.monacoEditor = stub;
      }
    });

    it("falls back to 0 for top() when no line is targeted", () => {
      component.lineNum = undefined;
      expect(component.top()).toBe(0);
      // left() does not depend on the line, so it still resolves.
      expect(component.left()).toBe(-120);
    });

    it("writes both css offsets when the target line changes", () => {
      mockUdfDebugService.getCondition.mockReturnValue("x > 1");
      component.lineNum = 3;
      const changes: SimpleChanges = {
        lineNum: { currentValue: 3, previousValue: 1, firstChange: false, isFirstChange: () => false },
      };

      component.ngOnChanges(changes);

      expect(component.topPosition).toBe("55px");
      expect(component.leftPosition).toBe("-120px");
    });

    it("substitutes zero offsets when the editor has no dom node and no glyph margin", () => {
      // Monaco reports no DOM node until the editor is attached, and a hidden
      // glyph margin reports 0. The remaining terms stay non-zero (line bottom
      // 40, scrollTop 5), so a stub that simply returned 0 everywhere would not
      // produce the numbers asserted below.
      mockUdfDebugService.getCondition.mockReturnValue("x > 1");
      component.monacoEditor = {
        getLayoutInfo: () => ({ glyphMarginLeft: 0 }),
        getDomNode: () => undefined,
        getBottomForLineNumber: () => 40,
        getScrollTop: () => 5,
        getScrollLeft: () => 0,
        dispose: vi.fn(),
      } as unknown as editor.IStandaloneCodeEditor;
      component.lineNum = 3;
      const changes: SimpleChanges = {
        lineNum: { currentValue: 3, previousValue: 1, firstChange: false, isFirstChange: () => false },
      };

      component.ngOnChanges(changes);

      // top: 0 (no rect) + 40 (line bottom) - 5 (scrollTop)
      expect(component.topPosition).toBe("35px");
      // left: 0 (no rect) + 0 (glyph margin) - 160 (popup width)
      expect(component.leftPosition).toBe("-160px");
      // top() reaches the same missing rect through its own fallback.
      expect(component.top()).toBe(35);
    });

    it("defaults the condition to empty when the debugger holds none for that line", () => {
      mockUdfDebugService.getCondition.mockReturnValue(undefined as unknown as string);
      component.lineNum = 4;
      const changes: SimpleChanges = {
        lineNum: { currentValue: 4, previousValue: 1, firstChange: false, isFirstChange: () => false },
      };
      // Seed a stale value first. `condition` initialises to "" on the class, so
      // without this the assertion below would also hold if ngOnChanges never
      // assigned anything at all -- it has to observe an overwrite.
      component.condition = "stale from the previous line";

      component.ngOnChanges(changes);

      // An unset breakpoint yields an empty box, not "undefined" in the input.
      expect(component.condition).toBe("");
    });
  });

  describe("the condition textarea", () => {
    const textarea = (): HTMLTextAreaElement => fixture.nativeElement.querySelector("textarea.condition-textarea");

    it("carries the current condition into the textarea", async () => {
      component.condition = "x > 1";
      fixture.detectChanges();
      await fixture.whenStable();

      expect(textarea().value).toBe("x > 1");
    });

    it("saves what the user types, not what the class was holding", async () => {
      // Every other test in this file writes `condition` from the class side, so the *inbound*
      // half of the two-way binding is all that is pinned: downgrade the template to a one-way
      // [ngModel] and the whole suite stays green while the popup silently discards every
      // keystroke. Drive a real input event and then let the save path read it back.
      component.condition = "stale";
      fixture.detectChanges();
      await fixture.whenStable();

      textarea().value = " y != 2 ";
      textarea().dispatchEvent(new Event("input"));

      expect(component.condition).toBe(" y != 2 ");

      component.handleEvent(new KeyboardEvent("keydown", { key: "Enter" }));

      expect(mockUdfDebugService.doUpdateBreakpointCondition).toHaveBeenCalledWith("test-operator", 1, "y != 2");
    });
  });

  describe("visibility", () => {
    it("is visible only while a line is targeted", () => {
      // The template keys its *ngIf on this, so an inverted getter leaves the popup stuck open.
      expect(component.isVisible).toBe(true);

      component.lineNum = undefined;
      expect(component.isVisible).toBe(false);
    });
  });

  it("ignores a keypress when no line is targeted", () => {
    component.lineNum = undefined;
    const emitted = vi.fn();
    component.closeEmitter.subscribe(emitted);

    component.handleEvent(new KeyboardEvent("keydown", { key: "Enter" }));

    // Neither saved nor closed: with no line there is nothing to attach a condition to.
    expect(mockUdfDebugService.doUpdateBreakpointCondition).not.toHaveBeenCalled();
    expect(emitted).not.toHaveBeenCalled();
  });

  it("should create the component", () => {
    expect(component).toBeTruthy();
  });

  it("should update the condition when lineNum changes", () => {
    mockUdfDebugService.getCondition.mockReturnValue("existing condition");

    const changes: SimpleChanges = {
      lineNum: {
        currentValue: 2,
        previousValue: 1,
        firstChange: false,
        isFirstChange: () => false,
      },
    };

    component.ngOnChanges(changes);

    expect(component.condition).toBe("existing condition");
  });

  it("should handle Enter key event and save the condition", () => {
    const emitSpy = vi.spyOn(component.closeEmitter, "emit");
    const event = new KeyboardEvent("keydown", { key: "Enter" });

    component.condition = " new condition ";
    component.handleEvent(event);

    expect(mockUdfDebugService.doUpdateBreakpointCondition).toHaveBeenCalledWith("test-operator", 1, "new condition");
    expect(emitSpy).toHaveBeenCalled();
  });

  it("should not handle Enter key event if shift key is pressed", () => {
    const emitSpy = vi.spyOn(component.closeEmitter, "emit");
    const event = new KeyboardEvent("keydown", { key: "Enter", shiftKey: true });

    component.handleEvent(event);

    expect(mockUdfDebugService.doUpdateBreakpointCondition).not.toHaveBeenCalled();
    expect(emitSpy).not.toHaveBeenCalled();
  });

  it("should emit close event on focusout", () => {
    const emitSpy = vi.spyOn(component.closeEmitter, "emit");

    component.handleEvent(); // Simulate focusout

    expect(emitSpy).toHaveBeenCalled();
  });

  it("saves the condition when Enter is pressed anywhere in the window", () => {
    // Every other test in this file calls handleEvent() directly, which leaves
    // the @HostListener("window:keydown") wiring itself unexercised: deleting
    // that decorator keeps the whole suite green. Drive a real window event.
    const emitSpy = vi.spyOn(component.closeEmitter, "emit");
    component.condition = " x > 1 ";

    window.dispatchEvent(new KeyboardEvent("keydown", { key: "Enter", cancelable: true }));

    expect(mockUdfDebugService.doUpdateBreakpointCondition).toHaveBeenCalledWith("test-operator", 1, "x > 1");
    expect(emitSpy).toHaveBeenCalled();
  });

  it("ignores an ordinary keystroke arriving at the window", () => {
    // The negative half of the same host binding, and the only test that pins
    // its ["$event"] argument list: without the argument, Angular calls
    // handleEvent() with no event, the key filter reads as focus-out, and every
    // keystroke anywhere in the window saves the condition and closes the popup.
    const emitSpy = vi.spyOn(component.closeEmitter, "emit");

    window.dispatchEvent(new KeyboardEvent("keydown", { key: "a", cancelable: true }));

    expect(mockUdfDebugService.doUpdateBreakpointCondition).not.toHaveBeenCalled();
    expect(emitSpy).not.toHaveBeenCalled();
  });

  it("saves and closes when the host element emits focusout", () => {
    // The twin of the keydown binding, and it has the same gap: the pre-existing
    // focusout test calls handleEvent() directly, so removing
    // @HostListener("focusout") leaves the whole suite green while the popup
    // silently stops saving on blur.
    const emitSpy = vi.spyOn(component.closeEmitter, "emit");
    component.condition = " y > 2 ";

    fixture.nativeElement.dispatchEvent(new Event("focusout"));

    expect(mockUdfDebugService.doUpdateBreakpointCondition).toHaveBeenCalledWith("test-operator", 1, "y > 2");
    expect(emitSpy).toHaveBeenCalled();
  });
});
