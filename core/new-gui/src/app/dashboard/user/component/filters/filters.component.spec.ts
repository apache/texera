import { ComponentFixture, TestBed } from "@angular/core/testing";

import { FiltersComponent } from "./filters.component";
import { StubOperatorMetadataService } from "src/app/workspace/service/operator-metadata/stub-operator-metadata.service";
import { OperatorMetadataService } from "src/app/workspace/service/operator-metadata/operator-metadata.service";
import { WorkflowPersistService } from "src/app/common/service/workflow-persist/workflow-persist.service";
import { StubWorkflowPersistService } from "src/app/common/service/workflow-persist/stub-workflow-persist.service";
import { testWorkflowEntries } from "../user-dashboard-test-fixtures";
import { HttpClient, HttpHandler } from "@angular/common/http";
import { NzDropDownModule } from "ng-zorro-antd/dropdown";

describe("FiltersComponent", () => {
  let component: FiltersComponent;
  let fixture: ComponentFixture<FiltersComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [FiltersComponent],
      providers: [
        HttpClient,
        HttpHandler,
        { provide: WorkflowPersistService, useValue: new StubWorkflowPersistService(testWorkflowEntries) },
        { provide: OperatorMetadataService, useClass: StubOperatorMetadataService },
      ],
      imports: [NzDropDownModule],
    }).compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(FiltersComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it("should create", () => {
    expect(component).toBeTruthy();
  });
});
