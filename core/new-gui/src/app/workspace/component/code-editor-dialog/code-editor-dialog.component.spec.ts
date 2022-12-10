import { ComponentFixture, TestBed, waitForAsync } from "@angular/core/testing";
import { CodeEditorDialogComponent } from "./code-editor-dialog.component";
import { MAT_DIALOG_DATA, MatDialogRef } from "@angular/material/dialog";
import { EMPTY } from "rxjs";
import { HttpClientTestingModule } from "@angular/common/http/testing";

describe("CodeEditorDialogComponent", () => {
  let component: CodeEditorDialogComponent;
  let fixture: ComponentFixture<CodeEditorDialogComponent>;

  beforeEach(
    waitForAsync(() => {
      TestBed.configureTestingModule({
        declarations: [CodeEditorDialogComponent],
        providers: [
          {
            provide: MatDialogRef,
            useValue: {
              keydownEvents: () => EMPTY,
              backdropClick: () => EMPTY,
            },
          },
          { provide: MAT_DIALOG_DATA, useValue: {} },
        ],
        imports: [HttpClientTestingModule],
      }).compileComponents();
    })
  );

  beforeEach(() => {
    fixture = TestBed.createComponent(CodeEditorDialogComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it("should create", () => {
    expect(component).toBeTruthy();
  });
});
