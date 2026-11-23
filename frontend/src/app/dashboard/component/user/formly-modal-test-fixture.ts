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

import { Component, Type } from "@angular/core";
import { ComponentFixture, TestBed, TestModuleMetadata } from "@angular/core/testing";
import { ReactiveFormsModule } from "@angular/forms";
import { BrowserAnimationsModule } from "@angular/platform-browser/animations";
import { FieldType, FieldTypeConfig, FormlyModule } from "@ngx-formly/core";
import { NzModalRef } from "ng-zorro-antd/modal";
import { commonTestProviders } from "../../../common/testing/test-utils";

// Formly still creates real controls and attaches the declared validators.
@Component({ template: "", standalone: true })
class StubFormlyFieldComponent extends FieldType<FieldTypeConfig> {}

/** Creates a TestBed component fixture with stubbed Formly field types and an NzModalRef close spy. */
export async function createFormlyModalFixture<T>(
  component: Type<T>,
  fieldNames: readonly string[],
  close: ReturnType<typeof vi.fn>,
  { imports = [], providers = [] }: Pick<TestModuleMetadata, "imports" | "providers"> = {}
): Promise<ComponentFixture<T>> {
  await TestBed.configureTestingModule({
    imports: [
      component,
      BrowserAnimationsModule,
      ReactiveFormsModule,
      FormlyModule.forRoot({ types: fieldNames.map(name => ({ name, component: StubFormlyFieldComponent })) }),
      ...imports,
    ],
    providers: [{ provide: NzModalRef, useValue: { close } }, ...providers, ...commonTestProviders],
  }).compileComponents();

  return TestBed.createComponent(component);
}
