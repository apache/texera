import { ComponentFixture, TestBed, waitForAsync } from "@angular/core/testing";

import { FeatureBarComponent } from "./feature-bar.component";
import { RouterTestingModule } from "@angular/router/testing";

import { MatDividerModule } from "@angular/material/divider";
import { MatListModule } from "@angular/material/list";
import { UserService } from "../../../common/service/user/user.service";
import { StubUserService } from "../../../common/service/user/stub-user.service";

describe("FeatureBarComponent", () => {
  let component: FeatureBarComponent;
  let fixture: ComponentFixture<FeatureBarComponent>;

  beforeEach(waitForAsync(() => {
    TestBed.configureTestingModule({
      declarations: [FeatureBarComponent],
      imports: [RouterTestingModule, MatDividerModule, MatListModule],
      providers: [{ provide: UserService, useClass: StubUserService }],
    }).compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(FeatureBarComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it("should create", () => {
    expect(component).toBeTruthy();
  });
});
