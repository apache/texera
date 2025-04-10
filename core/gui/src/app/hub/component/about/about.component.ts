import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { Component, OnInit } from "@angular/core";
import { environment } from "../../../../environments/environment";
import { UserService } from "src/app/common/service/user/user.service";
import { BehaviorSubject } from "rxjs";

@UntilDestroy()
@Component({
  selector: "texera-about",
  templateUrl: "./about.component.html",
  styleUrls: ["./about.component.scss"],
})
export class AboutComponent implements OnInit {
  localLogin = environment.localLogin;
  isLogin$ = new BehaviorSubject<boolean>(false); // control the visibility of the local login component

  constructor(private userService: UserService) {}

  ngOnInit() {
    this.isLogin$.next(this.userService.isLogin());
    // Subscribe to user changes
    this.userService
      .userChanged()
      .pipe(untilDestroyed(this))
      .subscribe(user => {
        this.isLogin$.next(user !== undefined);
      });
  }
}
