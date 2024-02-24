import { Component, OnInit } from "@angular/core";
import { WorkflowPersistService } from "../../../common/service/workflow-persist/workflow-persist.service";
import { UserService } from "../../../common/service/user/user.service";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { FlarumService } from "../service/flarum/flarum.service";

/**
 * dashboardComponent is the component which contains all the subcomponents
 * on the user dashboard. The subcomponents include Top bar, feature bar,
 * and feature container.
 *
 * @author Zhaomin Li
 */
@Component({
  selector: "texera-dashboard",
  templateUrl: "./dashboard.component.html",
  styleUrls: ["./dashboard.component.scss"],
  providers: [WorkflowPersistService],
})
@UntilDestroy()
export class DashboardComponent implements OnInit {
  isAdmin = this.userService.isAdmin();

  constructor(
    private userService: UserService,
    private flarumService: FlarumService
  ) {}

  ngOnInit(): void {
    if (!document.cookie.includes("flarum_remember")) {
      this.flarumService
        .auth()
        .pipe(untilDestroyed(this))
        .subscribe({
          next: (response: any) => {
            document.cookie = `flarum_remember=${response.token};path=/`;
          },
          error: (err: unknown) => {
            this.flarumService
              .register()
              .pipe(untilDestroyed(this))
              .subscribe(() => this.ngOnInit());
          },
        });
    }
  }
}
