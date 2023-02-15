import { ComponentFixture, TestBed } from "@angular/core/testing";

import { PortPropertyEditFrameComponent } from "./port-property-edit-frame.component";

describe("PortPropertyEditFrameComponent", () => {
  let component: PortPropertyEditFrameComponent;
  let fixture: ComponentFixture<PortPropertyEditFrameComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [PortPropertyEditFrameComponent],
    }).compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(PortPropertyEditFrameComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it("should create", () => {
    expect(component).toBeTruthy();
  });
});
