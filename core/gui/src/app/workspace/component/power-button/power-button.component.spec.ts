import { ComponentFixture, TestBed } from "@angular/core/testing";
import { HttpClientTestingModule } from "@angular/common/http/testing";
import { PowerButtonComponent } from "./power-button.component";
import { NzButtonModule } from "ng-zorro-antd/button";
import { CommonModule } from "@angular/common";
import { NzIconModule } from "ng-zorro-antd/icon";

describe("PowerButtonComponent", () => {
  let component: PowerButtonComponent;
  let fixture: ComponentFixture<PowerButtonComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [PowerButtonComponent], // Declare if not standalone
      imports: [
        HttpClientTestingModule, // Use TestingModule instead of HttpClientModule
        CommonModule,
        NzButtonModule,
        NzIconModule,
      ],
    }).compileComponents();

    fixture = TestBed.createComponent(PowerButtonComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it("should create", () => {
    expect(component).toBeTruthy();
  });
});
