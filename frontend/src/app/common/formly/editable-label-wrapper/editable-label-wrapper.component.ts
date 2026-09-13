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

import { Component } from "@angular/core";
import { NgIf } from "@angular/common";
import { NzIconDirective } from "ng-zorro-antd/icon";
import { FieldWrapper, FormlyFieldConfig } from "@ngx-formly/core";
import { merge } from "lodash-es";

/**
 * Lets an author rename or hide one field of the form in place: the label itself becomes
 * the input, so what they type is exactly what the reader sees, where they see it (the
 * schema's own labels -- "File Key", "Alias" -- describe the operator, not the reader's
 * task). Renders as a plain label for anyone not authoring.
 */
@Component({
  selector: "texera-editable-label-wrapper",
  templateUrl: "./editable-label-wrapper.component.html",
  styleUrls: ["./editable-label-wrapper.component.scss"],
  imports: [NgIf, NzIconDirective],
})
export class EditableLabelWrapperComponent extends FieldWrapper {
  /** Add this wrapper to a field with its naming + callbacks; `fallback` (the schema label)
   *  is the placeholder, so the author sees what leaving it blank yields. `rename` may be omitted
   *  for a reader mount (`authoring: false`), which renders a static label and no name input;
   *  `setHidden` may be omitted only with `canHide: false`, where the hide control is never
   *  rendered. */
  public static decorate(
    config: FormlyFieldConfig,
    state: { authoring: boolean; name: string; hidden: boolean; fallback: string; canHide?: boolean; group?: boolean },
    rename?: (name: string) => void,
    setHidden?: (hidden: boolean) => void
  ): void {
    merge(config, {
      wrappers: [...(config.wrappers ?? []), "editable-label-wrapper"],
      props: {
        ...config.props,
        // The wrapper draws the label itself; leaving formly's own label on would print
        // it twice.
        label: "",
        authoring: state.authoring,
        authorName: state.name,
        authorHidden: state.hidden,
        canHide: state.canHide !== false,
        // A repeated field has no labelable control carrying its id (the array widget renders rows
        // and buttons), so a `label for` would point at nothing; it is named as a group instead.
        labelsGroup: state.group === true,
        schemaLabel: state.fallback,
        renameField: rename,
        setFieldHidden: setHidden,
      },
    });
  }

  public onRename(event: Event): void {
    // The wrapper reflects the edit itself (the hidden label follows authorName), so the page does
    // not rebuild the form for a rename and the author keeps the focus where it is.
    const value = (event.target as HTMLInputElement).value;
    this.props["authorName"] = value;
    // Optional-chained for the same reason as setFieldHidden below: a reader mount omits rename and
    // never renders the input this handles, but the handler should not depend on that.
    this.props["renameField"]?.(value);
  }

  public onToggleHidden(): void {
    // Same as onRename: the eye, the pressed state and the faded field follow authorHidden here, so
    // no rebuild is needed and the focus stays on the eye.
    const hidden = !this.props["authorHidden"];
    this.props["authorHidden"] = hidden;
    // Optional-chained: decorate may omit setHidden when canHide is false, and although this handler
    // is unreachable then (the hide control is not rendered), a plain call would couple that to luck.
    this.props["setFieldHidden"]?.(hidden);
  }
}
