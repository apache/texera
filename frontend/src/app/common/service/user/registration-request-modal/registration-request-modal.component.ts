import { Component, Inject, Input, TemplateRef, ViewChild } from "@angular/core";
import { NZ_MODAL_DATA } from "ng-zorro-antd/modal";

@Component({
  selector: "texera-registration-request-modal",
  templateUrl: "./registration-request-modal.component.html",
  styleUrls: ["./registration-request-modal.component.scss"],
})
export class RegistrationRequestModalComponent {
  name = "";
  email = "";

  affiliation = "";
  reason = "";

  @ViewChild("modalTitle", { static: true })
  modalTitle!: TemplateRef<any>;

  constructor(
    @Inject(NZ_MODAL_DATA) public data: { uid: number; email: string; name: string }
  ) {
    this.name = data?.name ?? "";
    this.email = data?.email ?? "";
  }

  getValues() {
    return {
      affiliation: (this.affiliation ?? "").trim(),
      reason: (this.reason ?? "").trim(),
    };
  }
}
