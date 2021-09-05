import { ComponentFixture, TestBed } from "@angular/core/testing";

import { DebuggerFrameComponent } from "./debugger-frame.component";

describe("DebuggerFrameComponent", () => {
  let component: DebuggerFrameComponent;
  let fixture: ComponentFixture<DebuggerFrameComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [DebuggerFrameComponent]
    }).compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(DebuggerFrameComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it("should create", () => {
    expect(component).toBeTruthy();
  });
});
