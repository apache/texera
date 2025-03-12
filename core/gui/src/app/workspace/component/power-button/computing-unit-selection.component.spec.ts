import { ComponentFixture, TestBed } from "@angular/core/testing";
import { HttpClientTestingModule } from "@angular/common/http/testing";
import { ComputingUnitSelectionComponent } from "./computing-unit-selection.component";
import { NzButtonModule } from "ng-zorro-antd/button";
import { CommonModule } from "@angular/common";
import { NzIconModule } from "ng-zorro-antd/icon";

describe("PowerButtonComponent", () => {
  let component: ComputingUnitSelectionComponent;
  let fixture: ComponentFixture<ComputingUnitSelectionComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ComputingUnitSelectionComponent], // Declare if not standalone
      imports: [
        HttpClientTestingModule, // Use TestingModule instead of HttpClientModule
        CommonModule,
        NzButtonModule,
        NzIconModule,
      ],
    }).compileComponents();

    fixture = TestBed.createComponent(ComputingUnitSelectionComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it("should create", () => {
    expect(component).toBeTruthy();
  });
});
