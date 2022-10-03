import { Input, OnInit, Component, OnDestroy } from "@angular/core";
import { FieldType, FormlyFieldConfig } from '@ngx-formly/core';
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { UserFileService } from "src/app/dashboard/service/user-file/user-file.service";
import { OperatorPropertyEditFrameComponent } from "src/app/workspace/component/property-editor/operator-property-edit-frame/operator-property-edit-frame.component"

@UntilDestroy()
@Component({
  selector: "input-filename-autocomplete-template",
  templateUrl: "./input-filename-autocomplete-template.component.html",
  // styleUrls: ["input-filename-autocomplete-template.scss"],
})

// The FieldType<any> is a workaround for the issue of not assignable FormControl.
// details https://github.com/ngx-formly/ngx-formly/issues/2842#issuecomment-1066116865
// need to upgrade formly to v6 to properly fix this issue.
export class InputFilenameAutoCompleteComponent extends FieldType<any> {
  inputValue?: string;
  public selections: string[] = [];

  constructor(
    public userFileService: UserFileService,
    public operatorPropertyEditFrameComponent: OperatorPropertyEditFrameComponent,
  ) {
    super();
    if (this.operatorPropertyEditFrameComponent.formData.fileName != undefined)
      this.inputValue = this.operatorPropertyEditFrameComponent.formData.fileName;
  }

  onAutocomplete(event: Event): void {
    const value = (event.target as HTMLInputElement).value;
    this.selections = [];
    this.inputValue = value;
    if (value.length > 0) {
      this.userFileService.getAutoCompleteUserFileAccessList(value).pipe(untilDestroyed(this))
      .subscribe(autocompleteList => {
        this.selections = value ? autocompleteList.concat() : [];
        // To YunYan: do not change formData manually here. Form data should be updated by FormControl.
      });
    }
  }
}
