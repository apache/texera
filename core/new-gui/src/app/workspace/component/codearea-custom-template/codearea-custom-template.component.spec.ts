/* eslint-disable no-unused-vars, @typescript-eslint/no-unused-vars */
import { ComponentFixture, TestBed, waitForAsync } from "@angular/core/testing";
import { By } from "@angular/platform-browser";
import { DebugElement } from "@angular/core";

import { CodeareaCustomTemplateComponent } from "./codearea-custom-template.component";
import { MatDialog, MatDialogModule } from "@angular/material/dialog";
import { Overlay } from "@angular/cdk/overlay";

describe("CodeareaCustomTemplateComponent", () => {
  let component: CodeareaCustomTemplateComponent;
  let fixture: ComponentFixture<CodeareaCustomTemplateComponent>;

  beforeEach(
    waitForAsync(() => {
      TestBed.configureTestingModule({
        declarations: [CodeareaCustomTemplateComponent],
        imports: [MatDialogModule],
      }).compileComponents();
    })
  );

  beforeEach(() => {
    fixture = TestBed.createComponent(CodeareaCustomTemplateComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it("should create", () => {
    expect(component).toBeTruthy();
  });
});
