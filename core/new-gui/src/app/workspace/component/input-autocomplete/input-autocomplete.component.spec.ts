/* eslint-disable no-unused-vars, @typescript-eslint/no-unused-vars */
import { async, ComponentFixture, TestBed, waitForAsync } from "@angular/core/testing";
import { FormControl, FormGroup, ReactiveFormsModule } from '@angular/forms';
import { InputAutoCompleteComponent } from "./input-autocomplete.component";
import { MatDialogModule } from "@angular/material/dialog";
import { HttpClientTestingModule } from "@angular/common/http/testing";
import { WorkflowActionService } from "../../service/workflow-graph/model/workflow-action.service";
import { OperatorMetadataService } from "../../service/operator-metadata/operator-metadata.service";
import { StubOperatorMetadataService } from "../../service/operator-metadata/stub-operator-metadata.service";
import {UserFileService} from "src/app/dashboard/service/user-file/user-file.service";
import { HttpClientModule } from '@angular/common/http';

describe("InputAutoCompleteComponent", () => {
  let component: InputAutoCompleteComponent;
  let fixture: ComponentFixture<InputAutoCompleteComponent>;

  beforeEach(
    waitForAsync(() => {
      TestBed.configureTestingModule({
        declarations: [InputAutoCompleteComponent],
        imports: [ReactiveFormsModule, HttpClientTestingModule, HttpClientModule]
      }).compileComponents();
    })
  );

  beforeEach(() => {
    fixture = TestBed.createComponent(InputAutoCompleteComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it("should create", () => {
    expect(component).toBeTruthy();
  });
});
