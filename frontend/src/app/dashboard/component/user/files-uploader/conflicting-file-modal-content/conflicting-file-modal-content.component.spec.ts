// <Apache license header>
import { ComponentFixture, TestBed } from "@angular/core/testing";
import { NZ_MODAL_DATA } from "ng-zorro-antd/modal";
import { ConflictingFileModalContentComponent } from "./conflicting-file-modal-content.component";

describe("ConflictingFileModalContentComponent", () => {
  const data = { fileName: "a.csv", path: "/a.csv", size: "1 KB" };
  let component: ConflictingFileModalContentComponent;
  let fixture: ComponentFixture<ConflictingFileModalContentComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [ConflictingFileModalContentComponent],
      providers: [{ provide: NZ_MODAL_DATA, useValue: data }],
    }).compileComponents();
    fixture = TestBed.createComponent(ConflictingFileModalContentComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

    it("should create", () => {
    expect(component).toBeTruthy();
    });

    it("should expose injected modal data", () => {
    expect(component.data).toEqual(data);
    });
});