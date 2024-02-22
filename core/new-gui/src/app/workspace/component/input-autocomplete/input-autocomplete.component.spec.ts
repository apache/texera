/* eslint-disable no-unused-vars, @typescript-eslint/no-unused-vars */
import { ComponentFixture, TestBed, waitForAsync } from "@angular/core/testing";
import { FormControl, ReactiveFormsModule } from "@angular/forms";
import { InputAutoCompleteComponent } from "./input-autocomplete.component";
import { HttpClientTestingModule } from "@angular/common/http/testing";
import { HttpClientModule } from "@angular/common/http";

describe("InputAutoCompleteComponent", () => {
  let component: InputAutoCompleteComponent;
  let fixture: ComponentFixture<InputAutoCompleteComponent>;

  beforeEach(waitForAsync(() => {
    TestBed.configureTestingModule({
      declarations: [InputAutoCompleteComponent],
      imports: [ReactiveFormsModule, HttpClientTestingModule],
    }).compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(InputAutoCompleteComponent);
    component = fixture.componentInstance;
    component.field = { props: {}, formControl: new FormControl() };
    fixture.detectChanges();
  });

  it("should create", () => {
    expect(component).toBeTruthy();
  });
});
