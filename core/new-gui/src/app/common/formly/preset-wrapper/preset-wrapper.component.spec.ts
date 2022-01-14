// import { CommonModule } from "@angular/common";
// import { HttpClientTestingModule, HttpTestingController } from "@angular/common/http/testing";
// import { Component, ViewChild } from "@angular/core";
// import { ComponentFixture, fakeAsync, TestBed, tick } from "@angular/core/testing";
// import { FormGroup, ReactiveFormsModule } from "@angular/forms";
// import { BrowserModule, By } from "@angular/platform-browser";
// import { BrowserAnimationsModule } from "@angular/platform-browser/animations";
// import { FormlyFieldConfig, FormlyModule } from "@ngx-formly/core";
// import { FormlyMaterialModule } from "@ngx-formly/material";
// import { NzDropDownModule } from "ng-zorro-antd/dropdown";
// import { NzMenuModule } from "ng-zorro-antd/menu";
// import { NzMessageModule } from "ng-zorro-antd/message";
// import { PresetService } from "src/app/workspace/service/preset/preset.service";
// import { CustomNgMaterialModule } from "../../custom-ng-material.module";
// import { nonNull } from "../../util/assert";
// import { TEXERA_FORMLY_CONFIG } from "../formly-config";
// import { PresetWrapperComponent } from "./preset-wrapper.component";

// const testPreset = { testkey: "testPresetValue", otherkey: "otherPresetValue" };
// const fieldKey = "testkey";
// const presetKey = {
//   presetType: "testPresetType",
//   saveTarget: "testPresetSaveTarget",
//   applyTarget: "testPresetApplyTarget",
// };

// /**
//  * This mock component creates a formly form so that Formly api
//  * can be used to generate a form with the PresetWrapperComponent
//  */
// @Component({
//   selector: "texera-preset-test-cmp",
//   template: ` <form [formGroup]="form">
//     <formly-form [form]="form" [fields]="fields"> </formly-form>
//   </form>`,
// })
// class MockFormComponent {
//   @ViewChild(PresetWrapperComponent) child!: PresetWrapperComponent;
//   form = new FormGroup({});
//   fields: FormlyFieldConfig[] = [
//     {
//       wrappers: ["form-field", "preset-wrapper"],
//       key: fieldKey,
//       type: "input",
//       templateOptions: {
//         presetKey: presetKey,
//       },
//       defaultValue: "defaultValue",
//     },
//   ];
// }

// describe("PresetWrapperComponent", () => {
//   let component: PresetWrapperComponent;
//   let fixture: ComponentFixture<MockFormComponent>;
//   let httpMock: HttpTestingController;

//   beforeEach(() => {
//     TestBed.configureTestingModule({
//       declarations: [MockFormComponent, PresetWrapperComponent],
//       imports: [
//         CommonModule,
//         BrowserModule,
//         ReactiveFormsModule,
//         FormlyModule.forRoot(TEXERA_FORMLY_CONFIG),
//         FormlyMaterialModule,
//         CustomNgMaterialModule,
//         BrowserAnimationsModule,
//         HttpClientTestingModule,
//         NzMessageModule,
//         NzMenuModule,
//         NzDropDownModule,
//       ],
//     }).compileComponents();

//     fixture = TestBed.createComponent(MockFormComponent);
//     httpMock = TestBed.inject(HttpTestingController);
//     fixture.detectChanges();
//     component = fixture.debugElement.query(By.directive(PresetWrapperComponent)).componentInstance;
//   });

//   it("should create", () => {
//     expect(component).toBeTruthy();
//   });

//   describe("functional api", () => {
//     it("should properly apply a preset", () => {
//       const presetService = TestBed.inject(PresetService);
//       spyOn(presetService, "applyPreset");

//       component.applyPreset(testPreset);
//       expect(presetService.applyPreset).toHaveBeenCalledOnceWith(
//         presetKey.presetType,
//         presetKey.applyTarget,
//         testPreset
//       );
//     });

//     it("should properly delete a preset", () => {
//       const presetService = TestBed.inject(PresetService);
//       const otherPreset = { testkey: "otherPresetValue2", otherkey: "otherPresetValue2" };
//       const existingPresets = [testPreset, otherPreset];
//       spyOn(presetService, "getPresets").and.returnValue(existingPresets);
//       const deletePreset = spyOn(presetService, "deletePreset");

//       component.deletePreset(testPreset);
//       expect(deletePreset).toHaveBeenCalledTimes(1);
//       expect(deletePreset.calls.mostRecent().args.slice(0, 3)).toEqual([
//         presetKey.presetType,
//         presetKey.saveTarget,
//         testPreset,
//       ]);
//     });

//     it("should properly generate a preset title", () => {
//       expect(component.getEntryTitle(testPreset)).toEqual(jasmine.any(String));
//       expect(component.getEntryTitle(testPreset).replace(/\s\s+/g, "")).not.toEqual("");
//     });

//     it("should properly generate a preset description", () => {
//       expect(component.getEntryDescription(testPreset)).toEqual(jasmine.any(String));
//       expect(component.getEntryDescription(testPreset).replace(/\s\s+/g, "")).not.toEqual("");
//     });

//     it("should properly generate search results", () => {
//       expect(component.getSearchResults([testPreset], "", true)).toEqual([testPreset]);
//       expect(component.getSearchResults([testPreset], "asdf", true)).toEqual([testPreset]);
//       expect(component.getSearchResults([testPreset], component.getEntryTitle(testPreset), true)).toEqual([testPreset]);

//       expect(component.getSearchResults([testPreset], "", false)).toEqual([testPreset]);
//       expect(component.getSearchResults([testPreset], "asdf", false)).toEqual([]);
//       expect(component.getSearchResults([testPreset], component.getEntryTitle(testPreset), false)).toEqual([
//         testPreset,
//       ]);
//     });
//   });

//   describe("template bindings", () => {
//     it("should update search results when dropdown becomes visible", () => {
//       const debugElement = fixture.debugElement.query(By.directive(PresetWrapperComponent));
//       component.searchResults = [];
//       spyOn(component, "getSearchResults").and.returnValue([testPreset]);

//       expect(component.searchResults).toEqual([]);
//       // trigger nzVisibleChange, as if the dropdown menu was triggered
//       debugElement.query(By.css(".preset-field")).triggerEventHandler("nzVisibleChange", true);
//       fixture.detectChanges();
//       expect(component.searchResults).toEqual([testPreset]);
//     });

//     it("should generate an entry in the dropdown for each search result", fakeAsync(() => {
//       // recreate fixture and component in fakeAsync context so that event handlers will become synchronous
//       fixture = TestBed.createComponent(MockFormComponent);
//       fixture.detectChanges();
//       component = fixture.debugElement.query(By.directive(PresetWrapperComponent)).componentInstance;

//       const otherPreset = { testkey: "otherPresetValue2", otherkey: "otherPresetValue2" };
//       const searchResults = [testPreset, otherPreset];
//       const debugElement = fixture.debugElement.query(By.directive(PresetWrapperComponent));

//       // trigger dropdown menu
//       spyOn(component, "getSearchResults").and.returnValue(searchResults);
//       debugElement.query(By.css(".preset-field")).nativeElement.dispatchEvent(new Event("click"));
//       fixture.detectChanges();
//       tick(1000);
//       fixture.detectChanges();

//       const dropdown = nonNull(document.body.querySelector(".preset-menu"));
//       expect(dropdown.childElementCount).toEqual(component.searchResults.length);

//       // check that title and description of each dropdown entry match their preset
//       const nodes = dropdown.querySelectorAll("li");
//       for (let i = 0; i < dropdown.childElementCount; i++) {
//         let node = nodes[i];
//         let preset = searchResults[i];
//         expect(node.querySelector(".title")?.innerHTML).toEqual(component.getEntryTitle(preset));
//         expect(node.querySelector(".description")?.innerHTML).toEqual(component.getEntryDescription(preset));
//       }
//     }));

//     it("should apply the preset if a preset entry is clicked", fakeAsync(() => {
//       // recreate fixture and component in fakeAsync context so that event handlers will become synchronous
//       fixture = TestBed.createComponent(MockFormComponent);
//       fixture.detectChanges();
//       component = fixture.debugElement.query(By.directive(PresetWrapperComponent)).componentInstance;

//       const searchResults = [testPreset];
//       const debugElement = fixture.debugElement.query(By.directive(PresetWrapperComponent));
//       spyOn(component, "getSearchResults").and.returnValue(searchResults);
//       spyOn(component, "applyPreset");

//       // trigger dropdown menu
//       debugElement.query(By.css(".preset-field")).nativeElement.dispatchEvent(new Event("click"));
//       fixture.detectChanges();
//       tick(1000);
//       fixture.detectChanges();

//       const dropdown = nonNull(document.body.querySelector(".preset-menu"));
//       const dropdownEntry = nonNull(dropdown.querySelector(".dropdown-entry"));
//       expect(dropdown.childElementCount).toEqual(component.searchResults.length);
//       dropdownEntry.dispatchEvent(new Event("click"));
//       expect(component.applyPreset).toHaveBeenCalledOnceWith(testPreset);
//     }));

//     it("should delete the preset if a preset entry's delete button is clicked", fakeAsync(() => {
//       // recreate fixture and component in fakeAsync context so that event handlers will become synchronous
//       fixture = TestBed.createComponent(MockFormComponent);
//       fixture.detectChanges();
//       component = fixture.debugElement.query(By.directive(PresetWrapperComponent)).componentInstance;

//       const searchResults = [testPreset];
//       const debugElement = fixture.debugElement.query(By.directive(PresetWrapperComponent));
//       spyOn(component, "getSearchResults").and.returnValue(searchResults);
//       spyOn(component, "deletePreset");

//       // trigger dropdown menu
//       debugElement.query(By.css(".preset-field")).nativeElement.dispatchEvent(new Event("click"));
//       fixture.detectChanges();
//       tick(1000);
//       fixture.detectChanges();

//       // press delete button
//       const dropdown = nonNull(document.body.querySelector(".preset-menu"));
//       const dropdownDeleteButton = nonNull(dropdown.querySelector(".delete-button"));
//       expect(dropdown.childElementCount).toEqual(component.searchResults.length);
//       dropdownDeleteButton.dispatchEvent(new Event("click"));
//       expect(component.deletePreset).toHaveBeenCalledOnceWith(testPreset);
//     }));

//     it("should set new search results whenever the value of the field changes", fakeAsync(() => {
//       const inputfield = fixture.debugElement.query(By.css(".preset-field input")).nativeElement;
//       const searchResults = [testPreset];
//       spyOn(component, "getSearchResults").and.returnValue(searchResults);

//       // trigger input event as if typing
//       inputfield.value = "asdf";
//       inputfield.dispatchEvent(new Event("input"));
//       tick(1000);
//       expect(component.searchResults).toEqual(searchResults);
//     }));
//   });
// });
