import { Component, Input } from "@angular/core";

@Component({
  selector: "texera-hub",
  templateUrl: "hub.component.html",
  styleUrls: ["hub.component.scss"],
})
export class HubComponent {
  @Input() isLogin: boolean = false;
}
