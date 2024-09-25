import { ComponentFixture, TestBed } from "@angular/core/testing";
import { HttpClientModule } from "@angular/common/http";
import { PowerButtonComponent } from "./power-button.component";

describe("PowerButtonComponent", () => {
  let component: PowerButtonComponent;
  let fixture: ComponentFixture<PowerButtonComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [
        PowerButtonComponent, // Import standalone component
        HttpClientModule, // Import HttpClientModule for dynamic icon loading
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
