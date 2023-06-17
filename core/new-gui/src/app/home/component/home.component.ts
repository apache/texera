import { Component, OnDestroy, OnInit } from "@angular/core";
import { environment } from "../../../environments/environment";
import { UserService } from "../../common/service/user/user.service";
import { ActivatedRoute, Router } from "@angular/router";
import { GoogleService } from "../service/google.service";
import { mergeMap, takeUntil } from "rxjs/operators";
import { Subject } from "rxjs";

@Component({
  selector: "texera-login",
  templateUrl: "./home.component.html",
  styleUrls: ["./home.component.scss"],
})
export class HomeComponent implements OnInit, OnDestroy {
  localLogin = environment.localLogin;
  unsubscriber = new Subject();
  constructor(
    private userService: UserService,
    private router: Router,
    private route: ActivatedRoute,
    private googleService: GoogleService
  ) {}

  ngOnInit(): void {
    this.googleService.googleInit(document.getElementById("googleButton"));
    this.googleService.googleCredentialResponse
      .pipe(
        mergeMap(res => this.userService.googleLogin(res.credential)),
        takeUntil(this.unsubscriber)
      )
      .subscribe(
        Zone.current.wrap(() => {
          const url = this.route.snapshot.queryParams["returnUrl"] || "/dashboard/workflow";
          // TODO temporary solution: the new page will append to the bottom of the page, and the original page does not remove, zone solves this issue
          this.router.navigateByUrl(url);
        }, "")
      );
  }
  ngOnDestroy(): void {
    this.unsubscriber.next(1);
    this.unsubscriber.complete();
  }
}
