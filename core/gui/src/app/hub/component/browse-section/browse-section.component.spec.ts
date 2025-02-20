import { ComponentFixture, TestBed } from "@angular/core/testing";
import { BrowseSectionComponent } from "./browse-section.component";
import { WorkflowPersistService } from "../../../common/service/workflow-persist/workflow-persist.service";
import { DatasetService } from "../../../dashboard/service/user/dataset/dataset.service";
import { ChangeDetectorRef } from "@angular/core";

describe("BrowseSectionComponent", () => {
  let component: BrowseSectionComponent;
  let fixture: ComponentFixture<BrowseSectionComponent>;

  beforeEach(() => {
    TestBed.configureTestingModule({
      declarations: [BrowseSectionComponent],
      providers: [
        { provide: WorkflowPersistService, useValue: {} },
        { provide: DatasetService, useValue: {} },
        { provide: ChangeDetectorRef, useValue: {} },
      ],
    });
    fixture = TestBed.createComponent(BrowseSectionComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it("should create", () => {
    expect(component).toBeTruthy();
  });
});
