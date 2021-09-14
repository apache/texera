import { ComponentFixture, TestBed, waitForAsync } from "@angular/core/testing";

import { ResourceSectionComponent } from "./resource-section.component";

describe("ResourceSectionComponent", () => {
  let component: ResourceSectionComponent;
  let fixture: ComponentFixture<ResourceSectionComponent>;

  beforeEach(
    waitForAsync(() => {
      TestBed.configureTestingModule({
        declarations: [ResourceSectionComponent],
      }).compileComponents();
    })
  );

  beforeEach(() => {
    fixture = TestBed.createComponent(ResourceSectionComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it("should create", () => {
    expect(component).toBeTruthy();
  });
});
