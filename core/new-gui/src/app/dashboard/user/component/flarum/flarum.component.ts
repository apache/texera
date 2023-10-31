import { Component, OnInit } from "@angular/core";
import { DomSanitizer, SafeResourceUrl } from "@angular/platform-browser";
import { FlarumService } from "../../service/flarum/flarum.service";
import { catchError } from "rxjs/operators";
import { of } from "rxjs";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";

@UntilDestroy()
@Component({
  selector: "texera-app-flarum",
  templateUrl: "./flarum.component.html",
  styleUrls: ["./flarum.component.scss"],
})
export class FlarumComponent implements OnInit {
  flarumUrl: SafeResourceUrl | undefined;

  constructor(private sanitizer: DomSanitizer, private flarumService: FlarumService) {}

  ngOnInit(): void {
    this.authenticateAndRedirect();
  }

  authenticateAndRedirect(): void {
    const rawUrl = "http://localhost:80";
    if (!this.isFlarumAuthenticated()) {
      console.log("Not authenticated, redirecting to Flarum");
      this.flarumService
        .authenticateAndRedirect()
        .pipe(
          catchError((err: unknown) => {
            console.error("Error during authentication:", err);
            return of(null);
          }),
          untilDestroyed(this) // Auto-unsubscribe when the component is destroyed
        )
        .subscribe(() => {
          this.flarumUrl = this.sanitizer.bypassSecurityTrustResourceUrl(rawUrl);
          console.log("Successfully authenticated");
        });
    } else {
      this.flarumUrl = this.sanitizer.bypassSecurityTrustResourceUrl(rawUrl);
    }
  }

  isFlarumAuthenticated(): boolean {
    return document.cookie.includes("flarum_remember");
  }
}
