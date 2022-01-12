import { ComponentFixture, TestBed, waitForAsync } from "@angular/core/testing";

import { FeatureContainerComponent } from "./feature-container.component";
import { RouterTestingModule } from "@angular/router/testing";

describe("FeatureContainerComponent", () => {
  let component: FeatureContainerComponent;
  let fixture: ComponentFixture<FeatureContainerComponent>;

  beforeEach(
    waitForAsync(() => {
      TestBed.configureTestingModule({
        declarations: [FeatureContainerComponent],
        imports: [RouterTestingModule],
      }).compileComponents();
    })
  );

  beforeEach(() => {
    fixture = TestBed.createComponent(FeatureContainerComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it("should create", () => {
    expect(component).toBeTruthy();
  });
});
