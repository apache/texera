import { ComponentFixture, TestBed } from "@angular/core/testing";

import { CollabWrapperComponent } from "./collab-wrapper.component";

describe("CollabWrapperComponent", () => {
  let component: CollabWrapperComponent;
  let fixture: ComponentFixture<CollabWrapperComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [CollabWrapperComponent],
    }).compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(CollabWrapperComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it("should create", () => {
    expect(component).toBeTruthy();
  });
});
