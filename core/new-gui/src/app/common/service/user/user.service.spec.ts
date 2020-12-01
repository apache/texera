// import { TestBed } from '@angular/core/testing';
//
// import { UserService } from './user.service';
// import { HttpClientTestingModule, HttpTestingController } from '@angular/common/http/testing';
// import { UserWebResponse, UserWebResponseSuccess } from '../../type/user';
// import { AppSettings } from '../../app-setting';
//
// const userID = 1;
// const userName = 'test';
// const successCode = 0;
// const failedCode = 1;
//
// const successUserResponse: UserWebResponse = {
//   code: successCode,
//   user: {
//     name: userName,
//     uid: userID
//   }
// };
//
// const failedUserResponse: UserWebResponse = {
//   code: failedCode,
//   message: 'invalid user name or password'
// };
//
// const failedSessionLoginResponse: UserWebResponse = {
//   code: failedCode,
//   message: ''
// };
//
// // tslint:disable:no-non-null-assertion
// describe('UserService', () => {
//   let httpMock: HttpTestingController;
//   let service: UserService;
//
//   beforeEach(() => {
//     TestBed.configureTestingModule({
//       providers: [UserService],
//       imports: [
//         HttpClientTestingModule
//       ]
//     });
//     httpMock = TestBed.get(HttpTestingController);
//     service = TestBed.get(UserService);
//
//     // set default login from session response to a failure reponse
//     httpMock.expectOne(`${AppSettings.getApiEndpoint()}/${UserService.AUTH_STATUS_ENDPOINT}`)
//       .flush(failedSessionLoginResponse);
//   });
//
//   afterEach(() => {
//     httpMock.verify();
//   });
//
//   it('should be created', () => {
//     expect(service).toBeTruthy();
//   });
//
//   it('should login after register user', () => {
//     expect(service.getUser()).toBeFalsy();
//     service.register(userName).subscribe(
//       userWebResponse => {
//         expect(userWebResponse.code).toBe(successCode);
//         expect((userWebResponse as UserWebResponseSuccess).user.uid).toBe(userID);
//         expect((userWebResponse as UserWebResponseSuccess).user.name).toBe(userName);
//         expect(service.getUser()).toBeTruthy();
//       }
//     );
//
//     const req = httpMock.expectOne(`${AppSettings.getApiEndpoint()}/${UserService.REGISTER_ENDPOINT}`);
//     expect(req.request.method).toEqual('POST');
//     req.flush(successUserResponse);
//   });
//
//   it('should login after login user', () => {
//     expect(service.getUser()).toBeFalsy();
//     service.login(userName).subscribe(
//       userWebResponse => {
//         expect(userWebResponse.code).toBe(successCode);
//         expect((userWebResponse as UserWebResponseSuccess).user.uid).toBe(userID);
//         expect((userWebResponse as UserWebResponseSuccess).user.name).toBe(userName);
//         expect(service.getUser()).toBeTruthy();
//       }
//     );
//
//     const req = httpMock.expectOne(`${AppSettings.getApiEndpoint()}/${UserService.LOGIN_ENDPOINT}`);
//     expect(req.request.method).toEqual('POST');
//     req.flush(successUserResponse);
//   });
//
//   it('should get correct userID and userName after login', () => {
//     expect(service.getUser()).toBeFalsy();
//     service.login(userName).subscribe(
//       userWebResponse => {
//         expect(service.getUser()).toBeTruthy();
//         expect(service.getUser()!.uid).toBe(userID);
//         expect(service.getUser()!.name).toBe(userName);
//       }
//     );
//
//     const req = httpMock.expectOne(`${AppSettings.getApiEndpoint()}/${UserService.LOGIN_ENDPOINT}`);
//     expect(req.request.method).toEqual('POST');
//     req.flush(successUserResponse);
//   });
//
//   it('should not login after register failed', () => {
//     expect(service.getUser()).toBeFalsy();
//     service.register(userName).subscribe(
//       userWebResponse => {
//         expect(userWebResponse.code).toBe(failedCode);
//         expect(service.getUser()).toBeFalsy();
//       }
//     );
//
//     const req = httpMock.expectOne(`${AppSettings.getApiEndpoint()}/${UserService.REGISTER_ENDPOINT}`);
//     expect(req.request.method).toEqual('POST');
//     req.flush(failedUserResponse);
//   });
//
//   it('should not login after login failed', () => {
//     expect(service.getUser()).toBeFalsy();
//     service.login(userName).subscribe(
//       userWebResponse => {
//         expect(userWebResponse.code).toBe(failedCode);
//         expect(service.getUser()).toBeFalsy();
//       }
//     );
//
//     const req = httpMock.expectOne(`${AppSettings.getApiEndpoint()}/${UserService.LOGIN_ENDPOINT}`);
//     expect(req.request.method).toEqual('POST');
//     req.flush(failedUserResponse);
//   });
//
//   it('should return undefined when trying to get user field without not login', () => {
//     expect(service.getUser()).toBeFalsy();
//
//     service.login(userName).subscribe(
//       userWebResponse => {
//         expect(service.getUser()).toBeFalsy();
//       }
//     );
//
//     const req = httpMock.expectOne(`${AppSettings.getApiEndpoint()}/${UserService.LOGIN_ENDPOINT}`);
//     expect(req.request.method).toEqual('POST');
//     req.flush(failedUserResponse);
//   });
//
//   it('should raise error when trying to login again after login success', () => {
//     expect(service.getUser()).toBeFalsy();
//     service.login(userName).subscribe(
//       userWebResponse => {
//         expect(service.getUser()).toBeTruthy();
//         expect(() => service.login(userName)).toThrowError();
//         expect(() => service.register(userName)).toThrowError();
//       }
//     );
//
//     const req = httpMock.expectOne(`${AppSettings.getApiEndpoint()}/${UserService.LOGIN_ENDPOINT}`);
//     expect(req.request.method).toEqual('POST');
//     req.flush(successUserResponse);
//   });
//
//   it('should log out when called log out function', () => {
//     expect(service.getUser()).toBeFalsy();
//     service.login(userName).subscribe(
//       userWebResponse => {
//         expect(service.getUser()).toBeTruthy();
//         service.logOut();
//         // TODO Problems here, log out changes to communicate with backend so we can not test log out here.
//         // expect(service.getUser()).toBeFalsy();
//       }
//     );
//
//     const req = httpMock.expectOne(`${AppSettings.getApiEndpoint()}/${UserService.LOGIN_ENDPOINT}`);
//     expect(req.request.method).toEqual('POST');
//     req.flush(successUserResponse);
//     const req2 = httpMock.expectOne(`${AppSettings.getApiEndpoint()}/${UserService.LOG_OUT_ENDPOINT}`);
//     expect(req2.request.method).toEqual('GET');
//     req2.flush(successUserResponse);
//   });
//
//   it('should receive user change event when login', () => {
//     expect(service.getUser()).toBeFalsy();
//     service.getUserChangedEvent().subscribe(
//       () => expect(service.getUser()).toBeTruthy()
//     );
//
//     service.login(userName).subscribe();
//
//     const req = httpMock.expectOne(`${AppSettings.getApiEndpoint()}/${UserService.LOGIN_ENDPOINT}`);
//     expect(req.request.method).toEqual('POST');
//     req.flush(successUserResponse);
//   });
//
//   it('should receive user change event when register', () => {
//     expect(service.getUser()).toBeFalsy();
//     service.getUserChangedEvent().subscribe(
//       () => expect(service.getUser()).toBeTruthy()
//     );
//
//     service.register(userName).subscribe();
//
//     const req = httpMock.expectOne(`${AppSettings.getApiEndpoint()}/${UserService.REGISTER_ENDPOINT}`);
//     expect(req.request.method).toEqual('POST');
//     req.flush(successUserResponse);
//   });
//
//   it('should receive user change event when log out', () => {
//     expect(service.getUser()).toBeFalsy();
//     service.login(userName).subscribe(
//       () => {
//         expect(service.getUser()).toBeTruthy();
//         service.getUserChangedEvent().subscribe(
//           () => expect(service.getUser()).toBeFalsy()
//         );
//         service.logOut();
//       }
//     );
//
//     const req = httpMock.expectOne(`${AppSettings.getApiEndpoint()}/${UserService.LOGIN_ENDPOINT}`);
//     expect(req.request.method).toEqual('POST');
//     req.flush(successUserResponse);
//     const req2 = httpMock.expectOne(`${AppSettings.getApiEndpoint()}/${UserService.LOG_OUT_ENDPOINT}`);
//     expect(req2.request.method).toEqual('GET');
//     req2.flush(successUserResponse);
//   });
//
// });
//
//
// describe('UserService Session Login', () => {
//   let httpMock: HttpTestingController;
//   let service: UserService;
//
//   beforeEach(() => {
//     TestBed.configureTestingModule({
//       providers: [UserService],
//       imports: [
//         HttpClientTestingModule
//       ]
//     });
//     httpMock = TestBed.get(HttpTestingController);
//     service = TestBed.get(UserService);
//
//     // test login from session: don't flush login from session
//   });
//
//   afterEach(() => {
//     httpMock.verify();
//   });
//
//   it('should try automatic login from session on start up', () => {
//     let user;
//     service.getUserChangedEvent().subscribe(evt => user = evt);
//
//     const req = httpMock.expectOne(`${AppSettings.getApiEndpoint()}/${UserService.AUTH_STATUS_ENDPOINT}`);
//     req.flush(successUserResponse);
//     expect(service.getUser()).toBeTruthy();
//     expect(user).toBeTruthy();
//   });
//
//   it('should not emit user change event if login from session fails', () => {
//     let eventCount = 0;
//     service.getUserChangedEvent().subscribe(evt => eventCount++);
//
//     const req = httpMock.expectOne(`${AppSettings.getApiEndpoint()}/${UserService.AUTH_STATUS_ENDPOINT}`);
//     req.flush(failedUserResponse);
//     expect(service.getUser()).toBeFalsy();
//     expect(eventCount).toEqual(0);
//   });
// });
