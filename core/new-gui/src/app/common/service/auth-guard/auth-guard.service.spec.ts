import { TestBed } from "@angular/core/testing";

import { AuthGuardService } from "./auth-guard.service";
import { RouterTestingModule } from "@angular/router/testing";
import { HomeComponent } from "../../../home/component/home.component";
import { UserService } from "../user/user.service";
import { StubUserService } from "../user/stub-user.service";

describe("AuthGuardService", () => {
  let service: AuthGuardService;

  beforeEach(() => {
    TestBed.configureTestingModule({
      imports: [RouterTestingModule.withRoutes([{ path: "home", component: HomeComponent }])],
      providers: [AuthGuardService, { provide: UserService, useClass: StubUserService }],
    });
    service = TestBed.inject(AuthGuardService);
  });

  it("should be created", () => {
    expect(service).toBeTruthy();
  });
});
