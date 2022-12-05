import { FormControl, FormGroupDirective, NgForm } from "@angular/forms";
import { ErrorStateMatcher } from "@angular/material/core";

/**
 * Make Material mat-error always show error messages in forms even it's not focused
 * Solution from stackoverflow:
 * https://stackoverflow.com/questions/51456487/why-mat-error-not-get-displayed-inside-mat-form-field-in-angular-material-6-with/65542049#65542049
 */
export class AlwaysDisplayErrorStateMatcher implements ErrorStateMatcher {
  isErrorState(control: FormControl | null, form: FormGroupDirective | NgForm | null): boolean {
    // if (control == null || form == null) {
    //     return false;
    // }
    // const isSubmitted = form && form.submitted;
    return control !== null && control.invalid;
  }
}
