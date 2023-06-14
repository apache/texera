// import { HttpClientTestingModule, HttpTestingController } from "@angular/common/http/testing";
// import { TestBed, inject, fakeAsync, tick, flush, discardPeriodicTasks } from "@angular/core/testing";
// import { BrowserAnimationsModule } from "@angular/platform-browser/animations";
// import { NzMessageModule, NzMessageService } from "ng-zorro-antd/message";
// import { AppSettings } from "src/app/common/app-setting";
// import { DictionaryService } from "src/app/common/service/user/user-dictionary/dictionary.service";
// import { JointUIService } from "../joint-ui/joint-ui.service";
// import { OperatorMetadataService } from "../operator-metadata/operator-metadata.service";
// import { StubOperatorMetadataService } from "../operator-metadata/stub-operator-metadata.service";
// import { UndoRedoService } from "../undo-redo/undo-redo.service";
// import { WorkflowActionService } from "../workflow-graph/model/workflow-action.service";
// import { WorkflowUtilService } from "../workflow-graph/util/workflow-util.service";
// import { PresetService } from "./preset.service";
// import { mockPresetEnabledPredicate, mockPoint } from "../workflow-graph/model/mock-workflow-data";
// import { CustomJSONSchema7 } from "../../types/custom-json-schema.interface";
// import { mockPresetEnabledSchema } from "../operator-metadata/mock-operator-metadata.data";

// describe("PresetService", () => {
//   let presetService: PresetService;
//   let httpMock: HttpTestingController;

//   beforeEach(fakeAsync(() => {
//     TestBed.configureTestingModule({
//       providers: [
//         PresetService,
//         DictionaryService,
//         WorkflowActionService,
//         WorkflowUtilService,
//         JointUIService,
//         UndoRedoService,
//         { provide: OperatorMetadataService, useClass: StubOperatorMetadataService },
//       ],
//       imports: [NzMessageModule, HttpClientTestingModule, BrowserAnimationsModule],
//     });

//     presetService = TestBed.inject(PresetService);
//     httpMock = TestBed.inject(HttpTestingController);

//     // handle dict initialization
//     const testDict = { a: "a", b: "b", c: "c" };
//     const dictApiEndpoint = `${AppSettings.getApiEndpoint()}/${DictionaryService.USER_DICTIONARY_ENDPOINT}`;
//     httpMock.expectOne(`${AppSettings.getApiEndpoint()}/users/auth/status`).flush({ name: "testUser", uid: 1 }); // allow autologin by userService
//     httpMock.expectOne(`${dictApiEndpoint}/get`).flush({ code: 1, result: testDict });
//     httpMock.verify();
//     tick();
//   }));

//   it("should be created", inject([WorkflowActionService], (injectedService: WorkflowActionService) => {
//     expect(injectedService).toBeTruthy();
//   }));

//   describe("preset I/O", () => {
//     it("should emit an event when presets are applied", done => {
//       presetService.applyPresetStream.subscribe(value => {
//         expect(value).toEqual({ type: "testType", target: "testTarget", preset: { testPresetKey: "testPresetValue" } });
//         done();
//       });
//       presetService.applyPreset("testType", "testTarget", { testPresetKey: "testPresetValue" });
//     });

//     it("should emit an event when presets are saved", done => {
//       presetService.savePresetsStream.subscribe(value => {
//         expect(value).toEqual({
//           type: "testType",
//           target: "testTarget",
//           presets: [{ testPresetKey: "testPresetValue" }],
//         });
//         done();
//       });
//       presetService.savePresets("testType", "testTarget", [{ testPresetKey: "testPresetValue" }]);
//     });

//     it("should save to user dictionary when presets are saved", fakeAsync(() => {
//       const userDictionaryService = TestBed.inject(DictionaryService);
//       const dictApiEndpoint = `${AppSettings.getApiEndpoint()}/${DictionaryService.USER_DICTIONARY_ENDPOINT}`;

//       presetService.savePresets("testType", "testTarget", [{ testPresetKey: "testPresetValue" }]);
//       let savePresetReq = httpMock.expectOne(`${dictApiEndpoint}/set`);
//       expect(savePresetReq.cancelled).toBeFalsy();
//       expect(savePresetReq.request.method).toEqual("POST");
//       expect(savePresetReq.request.responseType).toEqual("json");
//       savePresetReq.flush({ code: 2, result: "arbitrary confirmation message" });
//       httpMock.verify();
//       tick();
//       flush();
//       expect(
//         userDictionaryService.getUserDictionary()[`${(PresetService as any).DICT_PREFIX}-testType-testTarget`]
//       ).toEqual(JSON.stringify([{ testPresetKey: "testPresetValue" }]));
//     }));

//     it("should save amended entry to user dictionary when a preset is deleted", fakeAsync(() => {
//       const userDictionaryService = TestBed.inject(DictionaryService);
//       const dictApiEndpoint = `${AppSettings.getApiEndpoint()}/${DictionaryService.USER_DICTIONARY_ENDPOINT}`;
//       const presetDictKey = `${(PresetService as any).DICT_PREFIX}-testType-testTarget`;
//       const initialPresets = [{ testPresetKey: "testPresetValue" }, { testPresetKey: "testPresetValue2" }];
//       const endPresets = initialPresets.slice(0, 1);

//       presetService.savePresets("testType", "testTarget", initialPresets);
//       let savePresetReq = httpMock.expectOne(`${dictApiEndpoint}/set`);
//       savePresetReq.flush({ code: 2, result: "arbitrary confirmation message" });
//       httpMock.verify();
//       tick();
//       flush();

//       expect(userDictionaryService.getUserDictionary()[presetDictKey]).toEqual(JSON.stringify(initialPresets));

//       presetService.deletePreset("testType", "testTarget", initialPresets[1]);
//       savePresetReq = httpMock.expectOne(`${dictApiEndpoint}/set`);
//       expect(savePresetReq.cancelled).toBeFalsy();
//       expect(savePresetReq.request.method).toEqual("POST");
//       expect(savePresetReq.request.responseType).toEqual("json");
//       savePresetReq.flush({ code: 2, result: "arbitrary confirmation message" });
//       httpMock.verify();
//       tick();
//       flush();
//       expect(userDictionaryService.getUserDictionary()[presetDictKey]).toEqual(JSON.stringify(endPresets));
//     }));

//     it("should save amended entry to user dictionary when a preset is updated", fakeAsync(() => {
//       const userDictionaryService = TestBed.inject(DictionaryService);
//       const dictApiEndpoint = `${AppSettings.getApiEndpoint()}/${DictionaryService.USER_DICTIONARY_ENDPOINT}`;
//       const presetDictKey = `${(PresetService as any).DICT_PREFIX}-testType-testTarget`;
//       const initialPresets = [{ testPresetKey: "testPresetValue" }, { testPresetKey: "testPresetValue2" }];
//       const updatedPreset = { testPresetKey: "testPresetValue3" };
//       const endPresets = [initialPresets[0], updatedPreset];

//       presetService.savePresets("testType", "testTarget", initialPresets);
//       let savePresetReq = httpMock.expectOne(`${dictApiEndpoint}/set`);
//       savePresetReq.flush({ code: 2, result: "arbitrary confirmation message" });
//       httpMock.verify();
//       tick();
//       flush();

//       expect(userDictionaryService.getUserDictionary()[presetDictKey]).toEqual(JSON.stringify(initialPresets));

//       presetService.updatePreset("testType", "testTarget", initialPresets[1], updatedPreset);
//       savePresetReq = httpMock.expectOne(`${dictApiEndpoint}/set`);
//       expect(savePresetReq.cancelled).toBeFalsy();
//       expect(savePresetReq.request.method).toEqual("POST");
//       expect(savePresetReq.request.responseType).toEqual("json");
//       savePresetReq.flush({ code: 2, result: "arbitrary confirmation message" });
//       httpMock.verify();
//       tick();
//       flush();
//       expect(userDictionaryService.getUserDictionary()[presetDictKey]).toEqual(JSON.stringify(endPresets));
//     }));

//     it("should delete from dictionary service when empty preset list is saved", fakeAsync(() => {
//       const userDictionaryService = TestBed.inject(DictionaryService);
//       const dictApiEndpoint = `${AppSettings.getApiEndpoint()}/${DictionaryService.USER_DICTIONARY_ENDPOINT}`;

//       presetService.savePresets("testType", "testTarget", []);
//       let savePresetReq = httpMock.expectOne(`${dictApiEndpoint}/delete`);
//       expect(savePresetReq.cancelled).toBeFalsy();
//       expect(savePresetReq.request.method).toEqual("DELETE");
//       expect(savePresetReq.request.responseType).toEqual("json");
//       savePresetReq.flush({ code: 2, result: "arbitrary confirmation message" });
//       httpMock.verify();
//       tick();
//       expect(
//         userDictionaryService.getUserDictionary()[`${(PresetService as any).DICT_PREFIX}-testType-testTarget`]
//       ).toBeUndefined();
//       flush();
//     }));

//     it("should get user presets from the user dictionary", fakeAsync(() => {
//       const userDictionaryService = TestBed.inject(DictionaryService);

//       // cant use an expression as a property name, so the dict must be setup via assignment :(
//       const testPresetKey = `${(PresetService as any).DICT_PREFIX}-testType-testTarget`;
//       const testPresets = [{ testPresetKey: "testPresetValue" }];
//       const testDict: any = {};
//       testDict[testPresetKey] = JSON.stringify(testPresets);

//       spyOn(userDictionaryService, "forceGetUserDictionary").and.returnValue(testDict);

//       presetService = new PresetService(
//         userDictionaryService,
//         TestBed.inject(NzMessageService),
//         TestBed.inject(WorkflowActionService),
//         TestBed.inject(OperatorMetadataService)
//       );

//       const presets = presetService.getPresets("testType", "testTarget");
//       expect(presets).toEqual(testPresets);
//     }));
//   });

//   describe("operator preset handling", () => {
//     let workflowActionService: WorkflowActionService;

//     beforeEach(() => {
//       workflowActionService = TestBed.inject(WorkflowActionService);
//       workflowActionService.addOperator(mockPresetEnabledPredicate, mockPoint);
//       workflowActionService.setOperatorProperty(mockPresetEnabledPredicate.operatorID, {
//         presetProperty: "testPresetProperty",
//         normalProperty: "testNormalProperty",
//       });
//     });

//     it("should not set operator properties if a non operator preset is applied", fakeAsync(() => {
//       presetService.applyPreset("NotAnOperator", "NotAnOperatorID", { NotAPresetProperty: "presetApplied" });
//       tick();

//       expect(
//         workflowActionService.getTexeraGraph().getOperator(mockPresetEnabledPredicate.operatorID).operatorProperties
//       ).toEqual({
//         presetProperty: "testPresetProperty",
//         normalProperty: "testNormalProperty",
//       });
//     }));

//     it("should not set operator properties if an invalid operator preset is applied", fakeAsync(() => {
//       expect(() => {
//         presetService.applyPreset("operator", mockPresetEnabledPredicate.operatorID, {
//           NotAPresetProperty: "presetApplied",
//         });
//         flush();
//       }).toThrow();

//       expect(
//         workflowActionService.getTexeraGraph().getOperator(mockPresetEnabledPredicate.operatorID).operatorProperties
//       ).toEqual({
//         presetProperty: "testPresetProperty",
//         normalProperty: "testNormalProperty",
//       });
//     }));

//     it("should set operator properties if a valid operator preset is applied", fakeAsync(() => {
//       presetService.applyPreset("operator", mockPresetEnabledPredicate.operatorID, { presetProperty: "presetApplied" });
//       tick();

//       expect(
//         workflowActionService.getTexeraGraph().getOperator(mockPresetEnabledPredicate.operatorID).operatorProperties
//       ).toEqual({
//         presetProperty: "presetApplied",
//         normalProperty: "testNormalProperty",
//       });
//     }));
//   });

//   describe("operator preset validation", () => {
//     beforeEach(() => {
//       const workflowActionService = TestBed.inject(WorkflowActionService);
//       workflowActionService.addOperator(mockPresetEnabledPredicate, mockPoint);
//     });

//     it("should reject an empty preset", () => {
//       expect(presetService.isValidOperatorPreset({}, mockPresetEnabledPredicate.operatorID)).toBeFalse();
//     });

//     it("should reject preset with the wrong properties", () => {
//       expect(
//         presetService.isValidOperatorPreset(
//           { wrongProperty: "wrongpropertyPreset" },
//           mockPresetEnabledPredicate.operatorID
//         )
//       ).toBeFalse();
//     });

//     it("should reject preset with empty properties", () => {
//       expect(
//         presetService.isValidOperatorPreset({ presetProperty: "" }, mockPresetEnabledPredicate.operatorID)
//       ).toBeFalse();
//     });

//     it("should accept a properly formatted preset", () => {
//       expect(
//         presetService.isValidOperatorPreset(
//           { presetProperty: "presetHasBeenApplied" },
//           mockPresetEnabledPredicate.operatorID
//         )
//       ).toBeTrue();
//     });

//     it("should reject new presets if they already exist", () => {
//       spyOn(presetService, "getPresets").and.returnValue([{ presetProperty: "presetHasBeenApplied" }]);

//       expect(
//         presetService.isValidNewOperatorPreset(
//           { presetProperty: "presetHasBeenApplied" },
//           mockPresetEnabledPredicate.operatorID
//         )
//       ).toBeFalse();
//     });

//     it("should accept new presets if they are novel", () => {
//       spyOn(presetService, "getPresets").and.returnValue([{ presetProperty: "presetHasBeenApplied" }]);

//       expect(
//         presetService.isValidNewOperatorPreset(
//           { presetProperty: "alternatePreset" },
//           mockPresetEnabledPredicate.operatorID
//         )
//       ).toBeTrue();
//     });
//   });

//   describe("operator preset schema generation", () => {
//     it("should generate a preset schema", () => {
//       const operatorSchema = <CustomJSONSchema7>{
//         type: "object",
//         properties: {
//           presetProperty: {
//             type: "string",
//             description: "property that can be saved in presets",
//             title: "presetProperty",
//             "enable-presets": true,
//           },
//           normalProperty: {
//             type: "string",
//             description: "property that is excluded in presets",
//             title: "normalProperty",
//           },
//         },
//         required: ["normalProperty"],
//       };

//       const presetSchema = <CustomJSONSchema7>{
//         type: "object",
//         properties: {
//           presetProperty: {
//             type: "string",
//             description: "property that can be saved in presets",
//             title: "presetProperty",
//             "enable-presets": true,
//           },
//         },
//         required: ["presetProperty"],
//         additionalProperties: false,
//       };

//       expect(PresetService.getOperatorPresetSchema(operatorSchema)).toEqual(presetSchema);
//     });

//     it("should throw an error if the operator schema has no properties", () => {
//       const operatorSchema = <CustomJSONSchema7>{
//         type: "object",
//         properties: {},
//         required: ["normalProperty"],
//       };

//       expect(() => PresetService.getOperatorPresetSchema(operatorSchema)).toThrow();
//     });

//     it("should throw an error if the operator schema has no preset properties", () => {
//       const operatorSchema = <CustomJSONSchema7>{
//         type: "object",
//         properties: {
//           normalProperty: {
//             type: "string",
//             description: "property that is excluded in presets",
//             title: "normalProperty",
//           },
//         },
//         required: ["normalProperty"],
//       };

//       expect(() => PresetService.getOperatorPresetSchema(operatorSchema)).toThrow();
//     });
//   });

//   describe("operator preset generation", () => {
//     describe("getOperatorPreset - throw errors if invalid", () => {
//       it("should throw an error if operator properties is empty", () => {
//         expect(() => PresetService.getOperatorPreset(mockPresetEnabledSchema.jsonSchema, {})).toThrow();
//       });

//       it("should throw an error if operator properties doesn't have all the preset properties", () => {
//         expect(() =>
//           PresetService.getOperatorPreset(mockPresetEnabledSchema.jsonSchema, { wrongProperty: "wrongpropertyPreset" })
//         ).toThrow();
//       });

//       it("should return a preset if operator properties has all the preset properties", () => {
//         expect(
//           PresetService.getOperatorPreset(mockPresetEnabledSchema.jsonSchema, { presetProperty: "presetPropertyValue" })
//         ).toEqual({ presetProperty: "presetPropertyValue" });
//       });

//       it("should return a preset if operator properties has a superset of preset properties", () => {
//         expect(
//           PresetService.getOperatorPreset(mockPresetEnabledSchema.jsonSchema, {
//             presetProperty: "presetPropertyValue",
//             otherProperty: "othervalue",
//           })
//         ).toEqual({ presetProperty: "presetPropertyValue" });
//       });
//     });

//     describe("filterOperatorPresetProperties - doesn't guarantee preset is valid", () => {
//       it("should never add to operator properties", () => {
//         expect(PresetService.filterOperatorPresetProperties(mockPresetEnabledSchema.jsonSchema, {})).toEqual({});
//       });

//       it("should filter out non preset properties", () => {
//         expect(
//           PresetService.filterOperatorPresetProperties(mockPresetEnabledSchema.jsonSchema, {
//             wrongProperty: "wrongpropertyPreset",
//           })
//         ).toEqual({});
//       });

//       it("should not filter out preset properties", () => {
//         expect(
//           PresetService.filterOperatorPresetProperties(mockPresetEnabledSchema.jsonSchema, {
//             presetProperty: "presetPropertyValue",
//           })
//         ).toEqual({ presetProperty: "presetPropertyValue" });
//       });

//       it("should filter out non preset properties and leave behind preset properties", () => {
//         expect(
//           PresetService.filterOperatorPresetProperties(mockPresetEnabledSchema.jsonSchema, {
//             presetProperty: "presetPropertyValue",
//             otherProperty: "othervalue",
//           })
//         ).toEqual({ presetProperty: "presetPropertyValue" });
//       });
//     });
//   });
// });
