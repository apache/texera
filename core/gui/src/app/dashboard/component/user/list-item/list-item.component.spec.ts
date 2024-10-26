import { ComponentFixture, TestBed } from "@angular/core/testing";
import { ListItemComponent } from "./list-item.component";
import { WorkflowPersistService } from "src/app/common/service/workflow-persist/workflow-persist.service";
import { HttpClientTestingModule } from "@angular/common/http/testing";
import { NzModalService } from "ng-zorro-antd/modal";
import { of, throwError } from "rxjs";
import { NO_ERRORS_SCHEMA } from "@angular/core";

describe("ListItemComponent", () => {
  let component: ListItemComponent;
  let fixture: ComponentFixture<ListItemComponent>;
  let workflowPersistService: jasmine.SpyObj<WorkflowPersistService>;

  beforeEach(async () => {
    const workflowPersistServiceSpy = jasmine.createSpyObj("WorkflowPersistService", [
      "updateWorkflowName",
      "updateWorkflowDescription",
    ]);

    await TestBed.configureTestingModule({
      imports: [HttpClientTestingModule],
      declarations: [ListItemComponent],
      providers: [{ provide: WorkflowPersistService, useValue: workflowPersistServiceSpy }, NzModalService],
      schemas: [NO_ERRORS_SCHEMA],
    }).compileComponents();

    fixture = TestBed.createComponent(ListItemComponent);
    component = fixture.componentInstance;
    workflowPersistService = TestBed.inject(WorkflowPersistService) as jasmine.SpyObj<WorkflowPersistService>;
  });

  it("should update workflow name successfully", () => {
    const newName = "New Workflow Name";
    component.entry = { id: 1, name: "Old Name" } as any;
    workflowPersistService.updateWorkflowName.and.returnValue(of({} as Response));

    component.confirmUpdateWorkflowCustomName(newName);

    expect(workflowPersistService.updateWorkflowName).toHaveBeenCalledWith(1, newName);
    expect(component.entry.name).toBe(newName);
    expect(component.editingName).toBeFalse();
  });

  it("should handle error when updating workflow name", () => {
    const newName = "New Workflow Name";
    component.entry = { id: 1, name: "Old Name" } as any;
    component.originalName = "Old Name";
    workflowPersistService.updateWorkflowName.and.returnValue(throwError(() => new Error("Error")));

    component.confirmUpdateWorkflowCustomName(newName);

    expect(workflowPersistService.updateWorkflowName).toHaveBeenCalledWith(1, newName);
    expect(component.entry.name).toBe("Old Name");
    expect(component.editingName).toBeFalse();
  });

  it("should update workflow description successfully", () => {
    const newDescription = "New Description";
    component.entry = { id: 1, description: "Old Description" } as any;
    workflowPersistService.updateWorkflowDescription.and.returnValue(of({} as Response));

    component.confirmUpdateWorkflowCustomDescription(newDescription);

    expect(workflowPersistService.updateWorkflowDescription).toHaveBeenCalledWith(1, newDescription);
    expect(component.entry.description).toBe(newDescription);
    expect(component.editingDescription).toBeFalse();
  });

  it("should handle error when updating workflow description", () => {
    const newDescription = "New Description";
    component.entry = { id: 1, description: "Old Description" } as any;
    component.originalDescription = "Old Description";
    workflowPersistService.updateWorkflowDescription.and.returnValue(throwError(() => new Error("Error")));

    component.confirmUpdateWorkflowCustomDescription(newDescription);

    expect(workflowPersistService.updateWorkflowDescription).toHaveBeenCalledWith(1, newDescription);
    expect(component.entry.description).toBe("Old Description");
    expect(component.editingDescription).toBeFalse();
  });
});
