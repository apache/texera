/* eslint-disable no-unused-vars, @typescript-eslint/no-unused-vars */
import { ComponentFixture, TestBed, waitForAsync } from "@angular/core/testing";

import { InputFilenameAutoCompleteComponent } from "./input-filename-autocomplete-template.component";
import { MatDialogModule } from "@angular/material/dialog";
import { HttpClientTestingModule } from "@angular/common/http/testing";
import { WorkflowActionService } from "../../service/workflow-graph/model/workflow-action.service";
import { OperatorMetadataService } from "../../service/operator-metadata/operator-metadata.service";
import { StubOperatorMetadataService } from "../../service/operator-metadata/stub-operator-metadata.service";

describe("InputFilenameAutoCompleteComponent", () => {
  let component: InputFilenameAutoCompleteComponent;
  let fixture: ComponentFixture<InputFilenameAutoCompleteComponent>;

  beforeEach(
    waitForAsync(() => {
      TestBed.configureTestingModule({
        declarations: [InputFilenameAutoCompleteComponent],
        imports: [MatDialogModule, HttpClientTestingModule],
        providers: [
          WorkflowActionService,
          {
            provide: OperatorMetadataService,
            useClass: StubOperatorMetadataService,
          },
        ],
      }).compileComponents();
    })
  );

  beforeEach(() => {
    fixture = TestBed.createComponent(InputFilenameAutoCompleteComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it("should create", () => {
    expect(component).toBeTruthy();
  });
});
