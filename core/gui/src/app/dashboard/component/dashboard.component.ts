import { ChangeDetectorRef, Component, NgZone, OnInit, ViewChild } from "@angular/core";
import { UserService } from "../../common/service/user/user.service";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { FlarumService } from "../service/user/flarum/flarum.service";
import { HttpErrorResponse } from "@angular/common/http";
import { ActivatedRoute, NavigationEnd, Router } from "@angular/router";
import { HubComponent } from "../../hub/component/hub.component";
import { GoogleAuthService } from "../../common/service/user/google-auth.service";
import { NotificationService } from "../../common/service/notification/notification.service";
import { catchError, mergeMap } from "rxjs/operators";
import { throwError } from "rxjs";

@Component({
  selector: "texera-dashboard",
  templateUrl: "dashboard.component.html",
  styleUrls: ["dashboard.component.scss"],
})
@UntilDestroy()
export class DashboardComponent implements OnInit {
  @ViewChild(HubComponent) hubComponent!: HubComponent;

  isAdmin: boolean = this.userService.isAdmin();
  isLogin = this.userService.isLogin();
  displayForum: boolean = true;
  displayNavbar: boolean = true;
  isCollpased: boolean = false;
  routesWithoutNavbar: string[] = ["/workspace", "/home", "/about"];
  showLinks: boolean = false;

  constructor(
    private userService: UserService,
    private router: Router,
    private flarumService: FlarumService,
    private cdr: ChangeDetectorRef,
    private route: ActivatedRoute,
    private googleAuthService: GoogleAuthService,
    private notificationService: NotificationService,
    private ngZone: NgZone
  ) {}

  ngOnInit(): void {
    this.isLogin = this.userService.isLogin();
    this.isAdmin = this.userService.isAdmin();

    this.isCollpased = false;

    if (!this.isLogin) {
      this.googleAuthService.googleAuthInit(document.getElementById("googleButton"));
      this.googleAuthService.googleCredentialResponse
        .pipe(mergeMap(res => this.userService.googleLogin(res.credential)))
        .pipe(
          catchError((e: unknown) => {
            this.notificationService.error((e as Error).message, { nzDuration: 10 });
            return throwError(() => e);
          })
        )
        .pipe(untilDestroyed(this))
        .subscribe(() => {
          this.ngZone.run(() => {
            this.router.navigateByUrl(this.route.snapshot.queryParams["returnUrl"] || "/dashboard/user/workflow");
          });
        });
    }
    if (!document.cookie.includes("flarum_remember") && this.isLogin) {
      this.flarumService
        .auth()
        .pipe(untilDestroyed(this))
        .subscribe({
          next: (response: any) => {
            document.cookie = `flarum_remember=${response.token};path=/`;
          },
          error: (err: unknown) => {
            if ((err as HttpErrorResponse).status == 404) {
              this.displayForum = false;
            } else {
              this.flarumService
                .register()
                .pipe(untilDestroyed(this))
                .subscribe(() => this.ngOnInit());
            }
          },
        });
    }
    this.router.events.pipe(untilDestroyed(this)).subscribe(() => {
      this.checkRoute();
    });

    this.router.events.pipe(untilDestroyed(this)).subscribe(event => {
      if (event instanceof NavigationEnd) {
        this.checkRoute();
        this.showLinks = event.url.includes("about");
      }
    });

    this.userService
      .userChanged()
      .pipe(untilDestroyed(this))
      .subscribe(() => {
        this.ngZone.run(() => {
          this.isLogin = this.userService.isLogin();
          this.isAdmin = this.userService.isAdmin();
          this.cdr.detectChanges();
        });
      });
  }

  checkRoute() {
    const currentRoute = this.router.url;
    this.displayNavbar = this.isNavbarEnabled(currentRoute);
  }

  isNavbarEnabled(currentRoute: string) {
    for (const routeWithoutNavbar of this.routesWithoutNavbar) {
      if (currentRoute.includes(routeWithoutNavbar)) {
        return false;
      }
    }
    return true;
  }

  handleCollapseChange(collapsed: boolean) {
    this.isCollpased = collapsed;
    const resizeEvent = new Event("resize");
    const editor = document.getElementById("workflow-editor");
    if (editor) {
      setTimeout(() => {
        window.dispatchEvent(resizeEvent);
      }, 175);
    }
  }
}
