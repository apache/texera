import { ComponentFixture, TestBed } from "@angular/core/testing";

import { BreakpointConditionInputComponent } from "./breakpoint-condition-input.component";
import { UdfDebugService } from "../../../service/operator-debug/udf-debug.service";
import { HttpClientTestingModule } from "@angular/common/http/testing";

describe("BreakpointConditionInputComponent", () => {
  let component: BreakpointConditionInputComponent;
  let fixture: ComponentFixture<BreakpointConditionInputComponent>;

  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [UdfDebugService],
      declarations: [BreakpointConditionInputComponent],
      imports: [HttpClientTestingModule],
    });
    fixture = TestBed.createComponent(BreakpointConditionInputComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it("should create", () => {
    expect(component).toBeTruthy();
  });
});
