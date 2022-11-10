import { ChangeDetectorRef, Component, OnInit } from "@angular/core";
import { UserService } from "../../../../../common/service/user/user.service";
import { FormBuilder, FormControl, FormGroup, Validators } from "@angular/forms";
import { isDefined } from "../../../../../common/util/predicate";
import { filter } from "rxjs/operators";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { NzModalRef } from "ng-zorro-antd/modal";

/**
 * UserLoginModalComponent is the pop up for user login/registration
 */
@UntilDestroy()
@Component({
  selector: "texera-user-login-modal",
  templateUrl: "./user-login-modal.component.html",
  styleUrls: ["./user-login-modal.component.scss"],
})
export class UserLoginModalComponent implements OnInit {

  constructor(private formBuilder: FormBuilder, public modal: NzModalRef, private userService: UserService) {
   
  }

  ngOnInit(): void {
        throw new Error("Method not implemented.");
    }
}
