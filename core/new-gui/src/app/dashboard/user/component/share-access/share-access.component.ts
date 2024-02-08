import { Component, Input, OnInit } from "@angular/core";
import { FormBuilder, FormGroup, Validators } from "@angular/forms";
import { ShareAccessService } from "../../service/share-access/share-access.service";
import { ShareAccess } from "../../type/share-access.interface";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { UserService } from "../../../../common/service/user/user.service";
import { GmailService } from "../../../admin/service/gmail.service";

@UntilDestroy()
@Component({
  templateUrl: "share-access.component.html",
})
export class ShareAccessComponent implements OnInit {
  @Input() writeAccess!: boolean;
  @Input() type!: string;
  @Input() id!: number;
  @Input() allOwners!: string[];

  public validateForm: FormGroup;
  public accessList: ReadonlyArray<ShareAccess> = [];
  public owner: string = "";
  public filteredOwners: Array<string> = [];
  public ownerSearchValue?: string;
  currentEmail: string | undefined = "";
  constructor(
    private accessService: ShareAccessService,
    private formBuilder: FormBuilder,
    private userService: UserService,
    private gmailService: GmailService
  ) {
    this.validateForm = this.formBuilder.group({
      email: [null, [Validators.email, Validators.required]],
      accessLevel: ["READ"],
    });
    this.currentEmail = this.userService.getCurrentUser()?.email;
  }

  ngOnInit(): void {
    this.accessService
      .getAccessList(this.type, this.id)
      .pipe(untilDestroyed(this))
      .subscribe(access => (this.accessList = access));
    this.accessService
      .getOwner(this.type, this.id)
      .pipe(untilDestroyed(this))
      .subscribe(name => {
        this.owner = name;
      });
  }

  public onChange(value: string): void {
    if (value === undefined) {
      this.filteredOwners = [];
    } else {
      this.filteredOwners = this.allOwners.filter(owner => owner.toLowerCase().indexOf(value.toLowerCase()) !== -1);
    }
  }

  public grantAccess(): void {
    if (this.validateForm.valid) {
      this.accessService
        .grantAccess(this.type, this.id, this.validateForm.value.email, this.validateForm.value.accessLevel)
        .pipe(untilDestroyed(this))
        .subscribe(() => {
          this.ngOnInit();
          this.gmailService.sendEmail(
            "Texera: " + this.owner + " shared a " + this.type + " with you",
            this.owner +
              " shared a " +
              this.type +
              " with you, access the workflow at " +
              location.origin +
              "/workflow/" +
              this.id,
            this.validateForm.value.email
          );
        });
    }
  }

  public revokeAccess(userToRemove: string): void {
    this.accessService
      .revokeAccess(this.type, this.id, userToRemove)
      .pipe(untilDestroyed(this))
      .subscribe(() => this.ngOnInit());
  }
}
