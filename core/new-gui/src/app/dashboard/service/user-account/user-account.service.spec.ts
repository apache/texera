import { TestBed, inject } from '@angular/core/testing';

import { UserAccountService, registerURL, loginURL } from './user-account.service';
import { HttpClientTestingModule, HttpTestingController } from '@angular/common/http/testing';
import { UserAccountResponse } from '../../type/user-account';
import { environment } from '../../../../environments/environment';

const userID = 1;
const userName = 'test';
const successCode = 0;
const failedCode = 1;

const successUserResponse: UserAccountResponse = {
  code : successCode,
  userAccount: {
    userName: userName,
    userID: userID
  },
  message: ''
};

const failedUserResponse: UserAccountResponse = {
  code : failedCode,
  userAccount: {
    userName: '',
    userID: -1
  },
  message: 'invalid user name or password'
};

describe('UserAccountService', () => {
  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [UserAccountService],
      imports: [
        HttpClientTestingModule
      ]
    });
    window.localStorage.clear();
  });

  afterEach(inject([HttpTestingController], (httpMock: HttpTestingController) => {
    httpMock.verify();
  }));

  it('should be created', inject([HttpTestingController, UserAccountService],
    (httpMock: HttpTestingController, service: UserAccountService) => {
    expect(service).toBeTruthy();
  }));

  it('should not login by default', inject([HttpTestingController, UserAccountService],
    (httpMock: HttpTestingController, service: UserAccountService) => {
    expect(service.isLogin()).toBeFalsy();
  }));

  it('should login after register user', inject([HttpTestingController, UserAccountService],
    (httpMock: HttpTestingController, service: UserAccountService) => {
      expect(service.isLogin()).toBeFalsy();
      service.registerUser(userName).subscribe(
        userAccountResponse => {
          expect(userAccountResponse.code).toBe(successCode);
          expect(userAccountResponse.userAccount.userID).toBe(userID);
          expect(userAccountResponse.userAccount.userName).toBe(userName);
          expect(service.isLogin()).toBeTruthy();
        }
      );

      const req = httpMock.expectOne(`${environment.apiUrl}/${registerURL}`);
      expect(req.request.method).toEqual('POST');
      req.flush(successUserResponse);
  }));

  it('should login after login user', inject([HttpTestingController, UserAccountService],
    (httpMock: HttpTestingController, service: UserAccountService) => {
      expect(service.isLogin()).toBeFalsy();
      service.loginUser(userName).subscribe(
        userAccountResponse => {
          expect(userAccountResponse.code).toBe(successCode);
          expect(userAccountResponse.userAccount.userID).toBe(userID);
          expect(userAccountResponse.userAccount.userName).toBe(userName);
          expect(service.isLogin()).toBeTruthy();
        }
      );

      const req = httpMock.expectOne(`${environment.apiUrl}/${loginURL}`);
      expect(req.request.method).toEqual('POST');
      req.flush(successUserResponse);
  }));

  it('should get correct userID and userName after login', inject([HttpTestingController, UserAccountService],
    (httpMock: HttpTestingController, service: UserAccountService) => {
      expect(service.isLogin()).toBeFalsy();
      service.loginUser(userName).subscribe(
        userAccountResponse => {
          expect(service.isLogin()).toBeTruthy();
          expect(service.getUserID()).toBe(userID);
          expect(service.getUserName()).toBe(userName);
        }
      );

      const req = httpMock.expectOne(`${environment.apiUrl}/${loginURL}`);
      expect(req.request.method).toEqual('POST');
      req.flush(successUserResponse);
  }));

  it('should not login after register failed', inject([HttpTestingController, UserAccountService],
    (httpMock: HttpTestingController, service: UserAccountService) => {
      expect(service.isLogin()).toBeFalsy();
      service.registerUser(userName).subscribe(
        userAccountResponse => {
          expect(userAccountResponse.code).toBe(failedCode);
          expect(service.isLogin()).toBeFalsy();
        }
      );

      const req = httpMock.expectOne(`${environment.apiUrl}/${registerURL}`);
      expect(req.request.method).toEqual('POST');
      req.flush(failedUserResponse);
  }));

  it('should not login after login failed', inject([HttpTestingController, UserAccountService],
    (httpMock: HttpTestingController, service: UserAccountService) => {
      expect(service.isLogin()).toBeFalsy();
      service.loginUser(userName).subscribe(
        userAccountResponse => {
          expect(userAccountResponse.code).toBe(failedCode);
          expect(service.isLogin()).toBeFalsy();
        }
      );

      const req = httpMock.expectOne(`${environment.apiUrl}/${loginURL}`);
      expect(req.request.method).toEqual('POST');
      req.flush(failedUserResponse);
  }));

  it('should raise error when trying to get user field without not login', inject([HttpTestingController, UserAccountService],
    (httpMock: HttpTestingController, service: UserAccountService) => {
      expect(service.isLogin()).toBeFalsy();
      expect(() => service.getUserID()).toThrowError();
      expect(() => service.getUserName()).toThrowError();

      service.loginUser(userName).subscribe(
        userAccountResponse => {
          expect(service.isLogin()).toBeFalsy();
          expect(() => service.getUserID()).toThrowError();
          expect(() => service.getUserName()).toThrowError();
        }
      );

      const req = httpMock.expectOne(`${environment.apiUrl}/${loginURL}`);
      expect(req.request.method).toEqual('POST');
      req.flush(failedUserResponse);
  }));

  it('should raise error when trying to login again after login success', inject([HttpTestingController, UserAccountService],
    (httpMock: HttpTestingController, service: UserAccountService) => {
      expect(service.isLogin()).toBeFalsy();
      service.loginUser(userName).subscribe(
        userAccountResponse => {
          expect(service.isLogin()).toBeTruthy();
          expect(() => service.loginUser(userName)).toThrowError();
          expect(() => service.registerUser(userName)).toThrowError();
        }
      );

      const req = httpMock.expectOne(`${environment.apiUrl}/${loginURL}`);
      expect(req.request.method).toEqual('POST');
      req.flush(successUserResponse);
  }));

  it('should log out when called log out function', inject([HttpTestingController, UserAccountService],
    (httpMock: HttpTestingController, service: UserAccountService) => {
      expect(service.isLogin()).toBeFalsy();
      service.loginUser(userName).subscribe(
        userAccountResponse => {
          expect(service.isLogin()).toBeTruthy();
          service.logOut();
          expect(service.isLogin()).toBeFalsy();
        }
      );

      const req = httpMock.expectOne(`${environment.apiUrl}/${loginURL}`);
      expect(req.request.method).toEqual('POST');
      req.flush(successUserResponse);
  }));

  it('should receive user change event when login', inject([HttpTestingController, UserAccountService],
    (httpMock: HttpTestingController, service: UserAccountService) => {
      expect(service.isLogin()).toBeFalsy();
      service.getUserChangeEvent().subscribe(
        () => expect(service.isLogin()).toBeTruthy()
      );

      service.loginUser(userName).subscribe();

      const req = httpMock.expectOne(`${environment.apiUrl}/${loginURL}`);
      expect(req.request.method).toEqual('POST');
      req.flush(successUserResponse);
  }));

  it('should receive user change event when register', inject([HttpTestingController, UserAccountService],
    (httpMock: HttpTestingController, service: UserAccountService) => {
      expect(service.isLogin()).toBeFalsy();
      service.getUserChangeEvent().subscribe(
        () => expect(service.isLogin()).toBeTruthy()
      );

      service.registerUser(userName).subscribe();

      const req = httpMock.expectOne(`${environment.apiUrl}/${registerURL}`);
      expect(req.request.method).toEqual('POST');
      req.flush(successUserResponse);
  }));

  it('should receive user change event when log out', inject([HttpTestingController, UserAccountService],
    (httpMock: HttpTestingController, service: UserAccountService) => {
      expect(service.isLogin()).toBeFalsy();
      service.loginUser(userName).subscribe(
        () => {
          expect(service.isLogin()).toBeTruthy();
          service.getUserChangeEvent().subscribe(
            () => expect(service.isLogin()).toBeFalsy()
          );
          service.logOut();
        }
      );

      const req = httpMock.expectOne(`${environment.apiUrl}/${loginURL}`);
      expect(req.request.method).toEqual('POST');
      req.flush(successUserResponse);
  }));


});
