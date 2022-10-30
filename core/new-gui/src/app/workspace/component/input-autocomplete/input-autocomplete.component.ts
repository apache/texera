import { Component, OnInit } from "@angular/core";
import { FieldType } from "@ngx-formly/core";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { UserFileService } from "src/app/dashboard/service/user-file/user-file.service";
import { FormControl } from "@angular/forms";
import { cloneDeep } from "lodash-es";
import { debounceTime } from "rxjs/operators";
import { isEqual } from "lodash";

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
  inputValue: string = "";
  title?: string;
  // the autocomplete selection list
  public suggestions: string[] = [];
  private userAccessableFileList: string[] = [];

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
      if (this.inputValue?.length > 0)
        this.userFileService
          .getAutoCompleteUserFileAccessList(encodeURIComponent(this.inputValue))
          .pipe(untilDestroyed(this))
          .subscribe(autocompleteList => {
            this.suggestions = [...autocompleteList];
          });
      else
        this.userFileService
          .retrieveDashboardUserFileEntryList()
          .pipe(untilDestroyed(this))
          .subscribe(list => {
            list.forEach(x => {
              this.userAccessableFileList.push(x.ownerName + "/" + x.file.name);
            });
            this.suggestions = [...this.userAccessableFileList];
          });
      this.title = this.field.templateOptions.label;
    }
  }

  equalsIgnoreOrder(a: string[], b: string[]): boolean {
    if (a.length !== b.length) return false;
    const uniqueValues = new Set([...a, ...b]);
    for (const v of uniqueValues) {
      const aCount = a.filter(e => e === v).length;
      const bCount = b.filter(e => e === v).length;
      if (aCount !== bCount) return false;
    }
    return true;
  }

  onAutocomplete(): void {
    // copy the input value
    const value = cloneDeep(this.inputValue);
    // used to get the selection list
    // TODO: currently it's a hard-code userfile service autocomplete
    this.userFileService
      .getAutoCompleteUserFileAccessList(encodeURIComponent(value))
      .pipe(debounceTime(300))
      .pipe(untilDestroyed(this))
      .subscribe(autocompleteList => {
        if (!this.equalsIgnoreOrder(this.suggestions, autocompleteList.concat()))
          this.suggestions = [...autocompleteList];
      });
  }
}
