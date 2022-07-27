import { ComponentFixture, TestBed } from "@angular/core/testing";

import { CoeditorUserIconComponent } from "./coeditor-user-icon.component";

describe("CoeditorUserIconComponent", () => {
  let component: CoeditorUserIconComponent;
  let fixture: ComponentFixture<CoeditorUserIconComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ CoeditorUserIconComponent ]
    })
    .compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(CoeditorUserIconComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it("should create", () => {
    expect(component).toBeTruthy();
  });
});
