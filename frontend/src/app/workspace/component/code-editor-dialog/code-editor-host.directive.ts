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

import { Directive, OnDestroy, ViewContainerRef } from "@angular/core";
import { CodeEditorService } from "../../service/code-editor/code-editor.service";

/**
 * Marks the outlet the code editor opens into, on an `<ng-template>` in a view's template. Each view
 * that shows operator properties -- the operator canvas and the Form View -- carries one, and the
 * view in front is the editor's host: its outlet is attached to CodeEditorService when the view's
 * template is built, and detached when the view is destroyed -- which closes an editor still open in
 * it too, since the view's teardown destroys what its outlet holds, before this hook runs. The views
 * are sibling routes and never overlap, so there is one host at a time; a late destroy still lets go
 * only of its own.
 */
@Directive({
  selector: "[texeraCodeEditorHost]",
  standalone: true,
})
export class CodeEditorHostDirective implements OnDestroy {
  constructor(
    private readonly outlet: ViewContainerRef,
    private readonly codeEditorService: CodeEditorService
  ) {
    this.codeEditorService.attachHost(this.outlet);
  }

  ngOnDestroy(): void {
    this.codeEditorService.detachHost(this.outlet);
  }
}
