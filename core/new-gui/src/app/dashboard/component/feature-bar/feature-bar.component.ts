import { Component } from "@angular/core";
import {User} from "../../../common/type/user";
import {NzModalService} from "ng-zorro-antd/modal";
import {UserService} from "../../../common/service/user/user.service";
import {UntilDestroy, untilDestroyed} from "@ngneat/until-destroy";

/**
 * FeatureBarComponent contains buttons for four main sections of the dashboard - Saved Project,
 * User Dictionary, Running Project, and Data Source.
 *
 * Each button links to one route stored in the 'app-routing.module'.
 * By clicking each button, a user can visit different sections in the feature container
 * (the path would change in corresponding to the button).
 *
 * @author Zhaomin Li
 */
@UntilDestroy()
@Component({
  selector: "texera-feature-bar",
  templateUrl: "./feature-bar.component.html",
  styleUrls: ["./feature-bar.component.scss"],
})
export class FeatureBarComponent {
  public permission: number | undefined;
  constructor(private modalService: NzModalService, private userService: UserService) {
    this.userService
      .userChanged()
      .pipe(untilDestroyed(this))
      .subscribe(user => (console.log(user?.permission)));
  }
}
