import { ComponentFixture, TestBed } from "@angular/core/testing";

import { BreakpointConditionInputComponent } from "./breakpoint-condition-input.component";

describe("BreakpointConditionInputComponent", () => {
  let component: BreakpointConditionInputComponent;
  let fixture: ComponentFixture<BreakpointConditionInputComponent>;

  beforeEach(() => {
    TestBed.configureTestingModule({
      declarations: [BreakpointConditionInputComponent],
    });
    fixture = TestBed.createComponent(BreakpointConditionInputComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it("should create", () => {
    expect(component).toBeTruthy();
  });
});
