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

import { Directive } from "@angular/core";
import { FieldType, FieldTypeConfig } from "@ngx-formly/core";

@Directive()
export abstract class HuggingFaceMediaUploadBase extends FieldType<FieldTypeConfig> {
  fileName = "";
  errorMessage = "";

  /** Data URL prefix the stored value must start with, e.g. `data:image/`. */
  protected abstract readonly dataUrlPrefix: string;
  /** Label shown when a value is present but no file name is known. */
  protected abstract readonly mediaFallbackLabel: string;

  get hasMedia(): boolean {
    const value = this.formControl.value;
    return typeof value === "string" && value.startsWith(this.dataUrlPrefix);
  }

  get previewSrc(): string {
    return this.hasMedia ? this.formControl.value : "";
  }

  get displayFileName(): string {
    if (this.fileName) return this.fileName;
    if (this.hasMedia) return this.mediaFallbackLabel;
    return "";
  }

  protected setMediaValue(dataUrl: string, fileName: string): void {
    this.fileName = fileName;
    this.formControl.setValue(dataUrl);
    if (typeof this.key === "string" && this.model) {
      this.model[this.key] = dataUrl;
    }
    this.formControl.markAsDirty();
    this.formControl.markAsTouched();
    this.formControl.updateValueAndValidity();
  }

  protected resetSelection(input: HTMLInputElement): void {
    this.fileName = "";
    input.value = "";
    this.formControl.setValue("");
    if (typeof this.key === "string" && this.model) {
      this.model[this.key] = "";
    }
    this.formControl.markAsDirty();
    this.formControl.markAsTouched();
    this.formControl.updateValueAndValidity();
  }

  clearMedia(input: HTMLInputElement): void {
    this.errorMessage = "";
    this.resetSelection(input);
  }
}
