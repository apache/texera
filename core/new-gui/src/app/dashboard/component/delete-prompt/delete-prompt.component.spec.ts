import { ComponentFixture, TestBed, waitForAsync } from "@angular/core/testing";

import { MatDialogModule } from "@angular/material/dialog";

import { NgbActiveModal, NgbModule } from "@ng-bootstrap/ng-bootstrap";
import { FormsModule } from "@angular/forms";
import { HttpClientModule } from "@angular/common/http";
import { DeletePromptComponent } from "./delete-prompt.component";

/**
 * Test deletion for workflow, project, file, and execution
 */
describe("DeletePromptComponent", () => {
  let deleteComponent: DeletePromptComponent;
  let deleteFixture: ComponentFixture<DeletePromptComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [DeletePromptComponent],
    }).compileComponents();
  });

  beforeEach(
    waitForAsync(() => {
      TestBed.configureTestingModule({
        declarations: [DeletePromptComponent],
        providers: [NgbActiveModal],
        imports: [MatDialogModule, NgbModule, FormsModule, HttpClientModule],
      }).compileComponents();
    })
  );

  beforeEach(() => {
    deleteFixture = TestBed.createComponent(DeletePromptComponent);
    deleteComponent = deleteFixture.componentInstance;
  });

  it("should create", () => {
    expect(deleteComponent).toBeTruthy();
  });

  it("deletion returns a result of true", () => {
    deleteFixture = TestBed.createComponent(DeletePromptComponent);
    deleteComponent = deleteFixture.componentInstance;
    deleteComponent.deletionType = "project"; //the string values do not change the test results
    deleteComponent.deletionName = "test entity";

    spyOn(deleteComponent.activeModal, "close");
    deleteComponent.delete();
    expect(deleteComponent.activeModal.close).toHaveBeenCalledWith(true);
  });
});
