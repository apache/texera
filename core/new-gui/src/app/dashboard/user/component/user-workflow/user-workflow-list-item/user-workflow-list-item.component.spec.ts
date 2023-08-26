import { ComponentFixture, TestBed, waitForAsync } from "@angular/core/testing";
import { UniquePipe } from "ngx-pipes";
import { UserWorkflowListItemComponent } from "./user-workflow-list-item.component";
import { FileSaverService } from "../../../service/user-file/file-saver.service";
import { testWorkflowEntries } from "../../user-dashboard-test-fixtures";
import { By } from "@angular/platform-browser";
import { StubWorkflowPersistService } from "src/app/common/service/workflow-persist/stub-workflow-persist.service";
import { WorkflowPersistService } from "src/app/common/service/workflow-persist/workflow-persist.service";
import { NgbActiveModal } from "@ng-bootstrap/ng-bootstrap";
import { HttpClient, HttpHandler } from "@angular/common/http";
import { UserProjectService } from "../../../service/user-project/user-project.service";
import { StubUserProjectService } from "../../../service/user-project/stub-user-project.service";

describe("UserWorkflowListItemComponent", () => {
  let component: UserWorkflowListItemComponent;
  let fixture: ComponentFixture<UserWorkflowListItemComponent>;
  const fileSaverServiceSpy = jasmine.createSpyObj<FileSaverService>(["saveAs"]);
  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [UserWorkflowListItemComponent, UniquePipe],
      providers: [
        { provide: WorkflowPersistService, useValue: new StubWorkflowPersistService(testWorkflowEntries) },
        { provide: UserProjectService, useValue: new StubUserProjectService() },
        NgbActiveModal,
        { provide: FileSaverService, useValue: fileSaverServiceSpy },
        HttpClient,
        HttpHandler,
      ],
    }).compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(UserWorkflowListItemComponent);
    component = fixture.componentInstance;
    component.entry = testWorkflowEntries[0];
    component.editable = true;
    fixture.detectChanges();
  });

  it("should create", () => {
    expect(component).toBeTruthy();
  });

  it("sends http request to backend to retrieve export json", () => {
    // Test the workflow download button.
    component.onClickDownloadWorkfllow();
    expect(fileSaverServiceSpy.saveAs).toHaveBeenCalledOnceWith(
      new Blob([JSON.stringify(testWorkflowEntries[0].workflow.workflow.content)], {
        type: "text/plain;charset=utf-8",
      }),
      "workflow 1.json"
    );
  });

  it("adding a workflow description adds a description to the workflow", waitForAsync(() => {
    fixture.whenStable().then(() => {
      let addWorkflowDescriptionBtn = fixture.debugElement.query(By.css(".add-description-btn"));
      expect(addWorkflowDescriptionBtn).toBeTruthy();
      addWorkflowDescriptionBtn.triggerEventHandler("click", null);
      fixture.detectChanges();
      let editableDescriptionInput = fixture.debugElement.nativeElement.querySelector(".workflow-editable-description");
      expect(editableDescriptionInput).toBeTruthy();
      spyOn(component, "confirmUpdateWorkflowCustomDescription");
      sendInput(editableDescriptionInput, "dummy description added by focusing out the input element.").then(() => {
        fixture.detectChanges();
        editableDescriptionInput.dispatchEvent(new Event("focusout"));
        fixture.detectChanges();
        expect(component.confirmUpdateWorkflowCustomDescription).toHaveBeenCalledTimes(1);
      });
    });
  }));

  it("Editing a workflow description edits a description to the workflow", waitForAsync(() => {
    fixture.whenStable().then(() => {
      const workflowDescriptionLabel = fixture.debugElement.query(By.css(".workflow-description"));
      expect(workflowDescriptionLabel).toBeTruthy();
      workflowDescriptionLabel.triggerEventHandler("click", null);
      fixture.detectChanges();
      let editableDescriptionInput1 = fixture.debugElement.nativeElement.querySelector(
        ".workflow-editable-description"
      );
      expect(editableDescriptionInput1).toBeTruthy();
      spyOn(component, "confirmUpdateWorkflowCustomDescription");
      sendInput(editableDescriptionInput1, "dummy description added by focusing out the input element.").then(() => {
        fixture.detectChanges();
        editableDescriptionInput1.dispatchEvent(new Event("focusout"));
        fixture.detectChanges();
        expect(component.confirmUpdateWorkflowCustomDescription).toHaveBeenCalledTimes(1);
      });
    });
  }));

  function sendInput(editableDescriptionInput: HTMLInputElement, text: string) {
    // Helper function to change the workflow description textbox.
    editableDescriptionInput.value = text;
    editableDescriptionInput.dispatchEvent(new Event("input"));
    fixture.detectChanges();
    return fixture.whenStable();
  }
});
