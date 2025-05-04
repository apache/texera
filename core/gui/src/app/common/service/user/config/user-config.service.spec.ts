/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

// import { HttpClientTestingModule, HttpTestingController } from "@angular/common/http/testing";
// import { fakeAsync, flush, inject, TestBed, tick } from "@angular/core/testing";
// import { AppSettings } from "src/app/common/app-setting";
// import { UserConfigService, UserConfig } from "./user-config.service";
// import { UserService } from "../user.service";
// import { StubUserService } from "../stub-user.service";

// describe("DictionaryService", () => {
//   let dictionaryService: UserConfigService;
//   let testDict: UserConfig;

//   beforeEach(() => {
//     TestBed.configureTestingModule({
//       providers: [{ provide: UserService, useClass: StubUserService }, UserConfigService],
//       imports: [HttpClientTestingModule],
//     });

//     dictionaryService = TestBed.inject(UserConfigService);
//     testDict = { a: "a", b: "b", c: "c" }; // sample dictionary used throughout testing
//   });

//   it("should be created", inject([UserConfigService], (injectedService: UserConfigService) => {
//     expect(injectedService).toBeTruthy();
//   }));

//   describe("Dictionary Service", () => {
//     describe("Backend interface", () => {
//       let httpMock: HttpTestingController;
//       let dictEventSubjectNextSpy: jasmine.Spy;

//       beforeEach(() => {
//         httpMock = TestBed.inject(HttpTestingController);
//         // handle the getAll() request created when initializing dictionaryService
//         httpMock.expectOne(`${AppSettings.getApiEndpoint()}/${UserConfigService.USER_DICTIONARY_ENDPOINT}`).flush({});

//         // clear dict
//         (dictionaryService as any).updateDict({});

//         dictEventSubjectNextSpy = spyOn((dictionaryService as any).dictionaryChangedSubject, "next");
//         dictEventSubjectNextSpy.calls.reset();
//       });

//       it("should produce a GET request when fetchKey() is called", fakeAsync(() => {
//         const testKey = "test";
//         dictionaryService.fetchKey(testKey);
//         // get() generates a POST request to this url
//         const req = httpMock.expectOne(
//           `${AppSettings.getApiEndpoint()}/${UserConfigService.USER_DICTIONARY_ENDPOINT}/${testKey}`
//         );
//         // POST request should have a properly formatted json payload
//         expect(req.request.method).toEqual("GET");
//         expect(req.request.responseType).toEqual("json");
//         req.flush("testValue");
//         flush();
//         httpMock.verify();
//         expect(dictEventSubjectNextSpy).toHaveBeenCalled();
//       }));

//       it("should produce a GET request when fetchAll() is called", fakeAsync(() => {
//         dictionaryService.fetchAll();
//         // getAll() generates a POST request to this url
//         const req = httpMock.expectOne(`${AppSettings.getApiEndpoint()}/${UserConfigService.USER_DICTIONARY_ENDPOINT}`);
//         // POST request should have a properly formatted json payload
//         expect(req.cancelled).toBeFalsy();
//         expect(req.request.method).toEqual("GET");
//         expect(req.request.responseType).toEqual("json");
//         req.flush({ testkey2: "testValue2" });
//         flush();
//         httpMock.verify();
//         expect(dictEventSubjectNextSpy).toHaveBeenCalled();
//       }));

//       it("should produce a PUT request when set() is called", fakeAsync(() => {
//         const testKey = "testkey3";
//         const testValue = "testValue3";
//         dictionaryService.set(testKey, testValue);
//         // set() generates a POST request to this url
//         const req = httpMock.expectOne(
//           `${AppSettings.getApiEndpoint()}/${UserConfigService.USER_DICTIONARY_ENDPOINT}/${testKey}`
//         );
//         // POST request should have a properly formatted json payload
//         expect(req.request.method).toEqual("PUT");
//         expect(req.request.body).toEqual({ value: testValue });
//         req.flush({});
//         flush();
//         httpMock.verify();
//         expect(dictEventSubjectNextSpy).toHaveBeenCalled();
//       }));

//       it("should produce a DELETE request when delete() is called", fakeAsync(() => {
//         const testKey = "testkey4";
//         dictionaryService.set(testKey, "testvalue4");
//         const setReq = httpMock.expectOne(
//           `${AppSettings.getApiEndpoint()}/${UserConfigService.USER_DICTIONARY_ENDPOINT}/${testKey}`
//         );
//         setReq.flush({});
//         dictEventSubjectNextSpy.calls.reset();

//         dictionaryService.delete(testKey);
//         // delete() generates a DELETE request to this url
//         const req = httpMock.expectOne(
//           `${AppSettings.getApiEndpoint()}/${UserConfigService.USER_DICTIONARY_ENDPOINT}/${testKey}`
//         );
//         // DELETE request should have a properly formatted json payload
//         expect(req.cancelled).toBeFalsy();
//         expect(req.request.method).toEqual("DELETE");
//         req.flush({});
//         flush();
//         httpMock.verify();
//         expect(dictEventSubjectNextSpy).toHaveBeenCalled();
//       }));
//     });
//   });
// });
