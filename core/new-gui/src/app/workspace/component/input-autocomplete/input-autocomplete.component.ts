import { Component } from "@angular/core";
import { FieldType } from "@ngx-formly/core";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { UserFileService } from "src/app/dashboard/service/user-file/user-file.service";
import { FormControl } from "@angular/forms";
import { debounceTime } from "rxjs/operators";
import { map } from "rxjs";

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
export class InputAutoCompleteComponent extends FieldType<any> {
  // the autocomplete selection list
  public suggestions: string[] = [];

  constructor(public userFileService: UserFileService) {
    super();
  }

  // FormControl is a non-nullable and read-only field.
  // This function is used to fit the test cases.
  getControl() {
    if (this.field == undefined) return new FormControl({});
    return this.formControl;
  }

  autocomplete(): void {
    if (this.field.formControl.value === null) {
      this.field.formControl.value = "";
    }
    // currently it's a hard-code UserFileService autocomplete
    // TODO: generalize this callback function with a formly hook.
    const value = this.field.formControl.value.trim();
    if (value.length > 0) {
      // perform auto-complete based on the current input
      this.userFileService
        .getAutoCompleteUserFileAccessList(value)
        .pipe(debounceTime(300))
        .pipe(untilDestroyed(this))
        .subscribe(suggestedFiles => {
          const updated =
            this.suggestions.length != suggestedFiles.length ||
            this.suggestions.some((e, i) => e !== suggestedFiles[i]);
          if (updated) this.suggestions = [...suggestedFiles];
        });
    } else {
      // no valid input, perform full scan
      this.userFileService
        .retrieveDashboardUserFileEntryList()
        .pipe(map(list => list.map(x => x.ownerName + "/" + x.file.name)))
        .pipe(untilDestroyed(this))
        .subscribe(allAccessibleFiles => (this.suggestions = allAccessibleFiles));
    }
  }
}
