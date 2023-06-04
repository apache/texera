import { ComponentFixture, TestBed } from "@angular/core/testing";

import { FiltersInstructionsComponent } from "./filters-instructions.component";

describe("FiltersInstructionsComponent", () => {
  let component: FiltersInstructionsComponent;
  let fixture: ComponentFixture<FiltersInstructionsComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [FiltersInstructionsComponent],
    }).compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(FiltersInstructionsComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it("should create", () => {
    expect(component).toBeTruthy();
  });
});
