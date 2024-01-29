import { Component, ViewChild, ViewContainerRef } from "@angular/core";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { FieldType } from "@ngx-formly/core";
import { CodeEditorComponent } from "../code-editor-dialog/code-editor.component";
import { CoeditorPresenceService } from "../../service/workflow-graph/model/coeditor-presence.service";
import { CodeEditorService } from "../../service/code-editor/code-editor.service";

/**
 * CodeareaCustomTemplateComponent is the custom template for 'codearea' type of formly field.
 *
 * When the formly field type is 'codearea', it overrides the default one line string input template
 * with this component.
 */
@UntilDestroy()
@Component({
  selector: "texera-codearea-custom-template",
  templateUrl: "codearea-custom-template.component.html",
  styleUrls: ["codearea-custom-template.component.scss"],
})
export class CodeareaCustomTemplateComponent extends FieldType<any> {
  constructor(private coeditorPresenceService: CoeditorPresenceService, private codeEditorService: CodeEditorService) {
    super();
    this.coeditorPresenceService
      .getCoeditorOpenedCodeEditorSubject()
      .pipe(untilDestroyed(this))
      .subscribe(_ => this.openEditor());
    this.coeditorPresenceService
      .getCoeditorClosedCodeEditorSubject()
      .pipe(untilDestroyed(this))
      .subscribe(_ => console.log("close editor"));
  }

  openEditor(): void {
    const componentRef = this.codeEditorService.vc.createComponent(CodeEditorComponent);
  }
}
