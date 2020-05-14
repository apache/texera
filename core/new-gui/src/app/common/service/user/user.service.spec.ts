import { AppSettings } from '../../app-setting';
import { TestBed, inject } from '@angular/core/testing';

import { UserService } from './user.service';
import { HttpClientTestingModule, HttpTestingController } from '@angular/common/http/testing';
import { UserWebResponse } from '../../type/user';
import { environment } from '../../../../environments/environment';

const userID = 1;
const userName = 'test';
const successCode = 0;
const failedCode = 1;

const successUserResponse: UserWebResponse = {
  code : successCode,
  user: {
    userName: userName,
    userID: userID
  },
  message: ''
};

const failedUserResponse: UserWebResponse = {
  code : failedCode,
  user: {
    userName: '',
    userID: -1
  },
  message: 'invalid user name or password'
};

// tslint:disable:no-non-null-assertion
describe('UserService', () => {
  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [UserService],
      imports: [
        HttpClientTestingModule
      ]
    });
    window.localStorage.clear();
  });

  afterEach(inject([HttpTestingController], (httpMock: HttpTestingController) => {
    httpMock.verify();
  }));

  it('should be created', inject([HttpTestingController, UserService],
    (httpMock: HttpTestingController, service: UserService) => {
    expect(service).toBeTruthy();
  }));

  it('should not login by default', inject([HttpTestingController, UserService],
    (httpMock: HttpTestingController, service: UserService) => {
    expect(service.getUser()).toBeFalsy();
  }));

  it('should login after register user', inject([HttpTestingController, UserService],
    (httpMock: HttpTestingController, service: UserService) => {
      expect(service.getUser()).toBeFalsy();
      service.register(userName).subscribe(
        userWebResponse => {
          expect(userWebResponse.code).toBe(successCode);
          expect(userWebResponse.user.userID).toBe(userID);
          expect(userWebResponse.user.userName).toBe(userName);
          expect(service.getUser()).toBeTruthy();
        }
      );

      const req = httpMock.expectOne(`${AppSettings.getApiEndpoint()}/${UserService.REGISTER_ENDPOINT}`);
      expect(req.request.method).toEqual('POST');
      req.flush(successUserResponse);
  }));

  it('should login after login user', inject([HttpTestingController, UserService],
    (httpMock: HttpTestingController, service: UserService) => {
      expect(service.getUser()).toBeFalsy();
      service.login(userName).subscribe(
        userWebResponse => {
          expect(userWebResponse.code).toBe(successCode);
          expect(userWebResponse.user.userID).toBe(userID);
          expect(userWebResponse.user.userName).toBe(userName);
          expect(service.getUser()).toBeTruthy();
        }
      );

      const req = httpMock.expectOne(`${AppSettings.getApiEndpoint()}/${UserService.LOGIN_ENDPOINT}`);
      expect(req.request.method).toEqual('POST');
      req.flush(successUserResponse);
  }));

  it('should get correct userID and userName after login', inject([HttpTestingController, UserService],
    (httpMock: HttpTestingController, service: UserService) => {
      expect(service.getUser()).toBeFalsy();
      service.login(userName).subscribe(
        userWebResponse => {
          expect(service.getUser()).toBeTruthy();
          expect(service.getUser()!.userID).toBe(userID);
          expect(service.getUser()!.userName).toBe(userName);
        }
      );

      const req = httpMock.expectOne(`${AppSettings.getApiEndpoint()}/${UserService.LOGIN_ENDPOINT}`);
      expect(req.request.method).toEqual('POST');
      req.flush(successUserResponse);
  }));

  it('should not login after register failed', inject([HttpTestingController, UserService],
    (httpMock: HttpTestingController, service: UserService) => {
      expect(service.getUser()).toBeFalsy();
      service.register(userName).subscribe(
        userWebResponse => {
          expect(userWebResponse.code).toBe(failedCode);
          expect(service.getUser()).toBeFalsy();
        }
      );

      const req = httpMock.expectOne(`${AppSettings.getApiEndpoint()}/${UserService.REGISTER_ENDPOINT}`);
      expect(req.request.method).toEqual('POST');
      req.flush(failedUserResponse);
  }));

  it('should not login after login failed', inject([HttpTestingController, UserService],
    (httpMock: HttpTestingController, service: UserService) => {
      expect(service.getUser()).toBeFalsy();
      service.login(userName).subscribe(
        userWebResponse => {
          expect(userWebResponse.code).toBe(failedCode);
          expect(service.getUser()).toBeFalsy();
        }
      );

      const req = httpMock.expectOne(`${AppSettings.getApiEndpoint()}/${UserService.LOGIN_ENDPOINT}`);
      expect(req.request.method).toEqual('POST');
      req.flush(failedUserResponse);
  }));

  it('should return undefiend when trying to get user field without not login', inject([HttpTestingController, UserService],
    (httpMock: HttpTestingController, service: UserService) => {
      expect(service.getUser()).toBeFalsy();

      service.login(userName).subscribe(
        userWebResponse => {
          expect(service.getUser()).toBeFalsy();
        }
      );

      const req = httpMock.expectOne(`${AppSettings.getApiEndpoint()}/${UserService.LOGIN_ENDPOINT}`);
      expect(req.request.method).toEqual('POST');
      req.flush(failedUserResponse);
  }));

  it('should raise error when trying to login again after login success', inject([HttpTestingController, UserService],
    (httpMock: HttpTestingController, service: UserService) => {
      expect(service.getUser()).toBeFalsy();
      service.login(userName).subscribe(
        userWebResponse => {
          expect(service.getUser()).toBeTruthy();
          expect(() => service.login(userName)).toThrowError();
          expect(() => service.register(userName)).toThrowError();
        }
      );

      const req = httpMock.expectOne(`${environment.apiUrl}/${UserService.LOGIN_ENDPOINT}`);
      expect(req.request.method).toEqual('POST');
      req.flush(successUserResponse);
  }));

  it('should log out when called log out function', inject([HttpTestingController, UserService],
    (httpMock: HttpTestingController, service: UserService) => {
      expect(service.getUser()).toBeFalsy();
      service.login(userName).subscribe(
        userWebResponse => {
          expect(service.getUser()).toBeTruthy();
          service.logOut();
          expect(service.getUser()).toBeFalsy();
        }
      );

      const req = httpMock.expectOne(`${environment.apiUrl}/${UserService.LOGIN_ENDPOINT}`);
      expect(req.request.method).toEqual('POST');
      req.flush(successUserResponse);
  }));

  it('should receive user change event when login', inject([HttpTestingController, UserService],
    (httpMock: HttpTestingController, service: UserService) => {
      expect(service.getUser()).toBeFalsy();
      service.getUserChangedEvent().subscribe(
        () => expect(service.getUser()).toBeTruthy()
      );

      service.login(userName).subscribe();

      const req = httpMock.expectOne(`${environment.apiUrl}/${UserService.LOGIN_ENDPOINT}`);
      expect(req.request.method).toEqual('POST');
      req.flush(successUserResponse);
  }));

  it('should receive user change event when register', inject([HttpTestingController, UserService],
    (httpMock: HttpTestingController, service: UserService) => {
      expect(service.getUser()).toBeFalsy();
      service.getUserChangedEvent().subscribe(
        () => expect(service.getUser()).toBeTruthy()
      );

      service.register(userName).subscribe();

      const req = httpMock.expectOne(`${environment.apiUrl}/${UserService.REGISTER_ENDPOINT}`);
      expect(req.request.method).toEqual('POST');
      req.flush(successUserResponse);
  }));

  it('should receive user change event when log out', inject([HttpTestingController, UserService],
    (httpMock: HttpTestingController, service: UserService) => {
      expect(service.getUser()).toBeFalsy();
      service.login(userName).subscribe(
        () => {
          expect(service.getUser()).toBeTruthy();
          service.getUserChangedEvent().subscribe(
            () => expect(service.getUser()).toBeFalsy()
          );
          service.logOut();
        }
      );

      const req = httpMock.expectOne(`${environment.apiUrl}/${UserService.LOGIN_ENDPOINT}`);
      expect(req.request.method).toEqual('POST');
      req.flush(successUserResponse);
  }));


});
