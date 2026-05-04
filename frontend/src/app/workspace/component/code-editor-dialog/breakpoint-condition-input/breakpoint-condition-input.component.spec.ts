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
describe("BreakpointConditionInputComponent", () => {
  let component: BreakpointConditionInputComponent;
  let fixture: ComponentFixture<BreakpointConditionInputComponent>;
  let mockUdfDebugService: any;

  beforeEach(async () => {
    // Create a mock UdfDebugService
    mockUdfDebugService = { getCondition: vi.fn(), doUpdateBreakpointCondition: vi.fn() };

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
    } as any;

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
});
