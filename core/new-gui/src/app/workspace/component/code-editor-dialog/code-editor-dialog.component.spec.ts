import { ComponentFixture, TestBed, waitForAsync } from "@angular/core/testing";
import { CodeEditorDialogComponent } from "./code-editor-dialog.component";
import { MAT_DIALOG_DATA, MatDialogRef } from "@angular/material/dialog";
import { EMPTY } from "rxjs";
import { HttpClientTestingModule } from "@angular/common/http/testing";
import { RouterTestingModule } from "@angular/router/testing";

describe("CodeEditorDialogComponent", () => {
  let component: CodeEditorDialogComponent;
  let fixture: ComponentFixture<CodeEditorDialogComponent>;

  beforeEach(waitForAsync(() => {
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
      imports: [RouterTestingModule, HttpClientTestingModule],
    }).compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(CodeEditorDialogComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it("should create", () => {
    expect(component).toBeTruthy();
  });

  // it("should create a websocket when the editor is opened", () => {
  //   let socketInstance = component.getLanguageServerSocket();
  //   expect(socketInstance).toBeTruthy();
  // });
});
