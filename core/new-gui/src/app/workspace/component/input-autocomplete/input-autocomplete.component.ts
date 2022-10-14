import { Component, OnInit } from "@angular/core";
import { FieldType } from "@ngx-formly/core";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { UserFileService } from "src/app/dashboard/service/user-file/user-file.service";
import { FormControl } from "@angular/forms";

@UntilDestroy()
@Component({
  selector: "texera-input-autocomplete-template",
  templateUrl: "./input-autocomplete.component.html",
  styleUrls: ["input-autocomplete.component.scss"],
})

/* *
 * The FieldType<any> is a workaround for the issue of not assignable FormControl.
 * details https://github.com/ngx-formly/ngx-formly/issues/2842#issuecomment-1066116865
 * need to upgrade formly to v6 to properly fix this issue.
 */
export class InputAutoCompleteComponent extends FieldType<any> implements OnInit {
  inputValue?: string = "";
  title?: string;
  // the autocomplete selection list
  public selections: string[] = [];

  constructor(public userFileService: UserFileService) {
    super();
  }

  // TODO: write this function because formcontrol is a nonnullable and read-only field. This is used to fit the test.
  getControl() {
    if (this.field == undefined) return new FormControl({});
    return this.formControl;
  }

  ngOnInit() {
    if (this.field) {
      this.inputValue = this.field.formControl.value;
      this.userFileService
      .getAutoCompleteUserFileAccessList("")
      .pipe(untilDestroyed(this))
      .subscribe(autocompleteList => {
        this.selections = autocompleteList.concat();
      });
      this.title = this.field.templateOptions.label;
    }
  }

  onAutocomplete(): void {
    this.selections = [];
    // copy the input value
    const value = JSON.parse(JSON.stringify(this.inputValue));
    // used to get the selection list
    // TODO: currently it's a hard-code userfile service autocomplete
    this.userFileService
      .getAutoCompleteUserFileAccessList(value)
      .pipe(untilDestroyed(this))
      .subscribe(autocompleteList => {
        this.selections = autocompleteList.concat();
      });
  }
}
