import { Component } from "@angular/core";
import { DomSanitizer, SafeResourceUrl } from "@angular/platform-browser";

@Component({
  templateUrl: "./flarum.component.html",
})
export class FlarumComponent {
  flarumUrl: SafeResourceUrl = this.sanitizer.bypassSecurityTrustResourceUrl("http://localhost");
  constructor(private sanitizer: DomSanitizer) {}
}
