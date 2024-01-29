import { ComponentFixture, TestBed, waitForAsync } from "@angular/core/testing";
import { CodeEditorComponent } from "./code-editor.component";
import { MAT_DIALOG_DATA, MatDialogRef } from "@angular/material/dialog";
import { EMPTY } from "rxjs";
import { HttpClientTestingModule } from "@angular/common/http/testing";

describe("CodeEditorDialogComponent", () => {
  let component: CodeEditorComponent;
  let fixture: ComponentFixture<CodeEditorComponent>;

  beforeEach(waitForAsync(() => {
    TestBed.configureTestingModule({
      declarations: [CodeEditorComponent],
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
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(CodeEditorComponent);
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
