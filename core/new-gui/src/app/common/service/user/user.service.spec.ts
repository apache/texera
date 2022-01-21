import { fakeAsync, TestBed, tick } from "@angular/core/testing";

import { UserService } from "./user.service";
import { AuthService } from "./auth.service";
import { StubAuthService } from "./stub-auth.service";
import { skip } from "rxjs/operators";

describe("UserService", () => {
  let service: UserService;

  beforeEach(() => {
    AuthService.removeAccessToken();
    TestBed.configureTestingModule({
      providers: [UserService, { provide: AuthService, useClass: StubAuthService }],
    });

    service = TestBed.inject(UserService);
  });

  afterAll(() => {
    TestBed.resetTestingModule();
  });

  it("should be created", () => {
    expect(service).toBeTruthy();
  });

  it("should login after register user", () => {
    expect((service as any).currentUser).toBeFalsy();
    service
      .userChanged()
      .pipe(skip(1))
      .subscribe(user => expect(user).toBeTruthy());
    service.register("test", "password").subscribe(() => {
      expect((service as any).currentUser).toBeTruthy();
    });
  });

  it("should login after login user", () => {
    expect((service as any).currentUser).toBeFalsy();
    service
      .userChanged()
      .pipe(skip(1))
      .subscribe(user => expect(user).toBeTruthy());
    service.login("test", "password").subscribe(() => {
      expect((service as any).currentUser).toBeTruthy();
    });
  });

  it("should not login after register failed", () => {
    expect((service as any).currentUser).toBeFalsy();
    service
      .userChanged()
      .pipe(skip(1))
      .subscribe(user => expect(user).toBeFalsy());
    service.register("existing_user", "password").subscribe(() => {
      expect((service as any).currentUser).toBeFalsy();
    });
  });

  it("should not login after login failed", () => {
    expect((service as any).currentUser).toBeFalsy();
    service
      .userChanged()
      .pipe(skip(1))
      .subscribe(user => expect(user).toBeFalsy());
    service.login("test", "wrong_password").subscribe(() => {
      expect((service as any).currentUser).toBeFalsy();
    });
  });

  it("should log out when called log out function", fakeAsync(() => {
    expect((service as any).currentUser).toBeFalsy();
    service
      .userChanged()
      .pipe(skip(2))
      .subscribe(user => expect(user).toBeFalsy());
    service.login("test", "password").subscribe(() => {
      expect((service as any).currentUser).toBeTruthy();

      tick(10);
      service.logout();

      tick(10);
      expect((service as any).currentUser).toBeFalsy();
    });
  }));
});
