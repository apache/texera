import { ComponentFixture, TestBed, waitForAsync } from "@angular/core/testing";

import { RunningJobSectionComponent } from "./running-job-section.component";

describe("RunningJobSectionComponent", () => {
  let component: RunningJobSectionComponent;
  let fixture: ComponentFixture<RunningJobSectionComponent>;

  beforeEach(
    waitForAsync(() => {
      TestBed.configureTestingModule({
        declarations: [RunningJobSectionComponent],
      }).compileComponents();
    })
  );

  beforeEach(() => {
    fixture = TestBed.createComponent(RunningJobSectionComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it("should create", () => {
    expect(component).toBeTruthy();
  });
});
