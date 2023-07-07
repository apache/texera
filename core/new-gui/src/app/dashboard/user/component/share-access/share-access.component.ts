import { Component, Input, OnInit } from "@angular/core";
import { NgbActiveModal } from "@ng-bootstrap/ng-bootstrap";
import { FormBuilder, Validators } from "@angular/forms";
import { ShareAccessService } from "../../service/share-access/share-access.service";
import { ShareAccess } from "../../type/share-access.interface";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { UserService } from "../../../../common/service/user/user.service";

@UntilDestroy()
@Component({
  templateUrl: "share-access.component.html",
  styleUrls: ["share-access.component.scss"],
  providers: [{ provide: "type", useValue: "workflow" }],
})
export class ShareAccessComponent implements OnInit {
  @Input() writeAccess!: boolean;
  @Input() type!: string;
  @Input() id!: number;
  @Input() allOwners!: string[];

  validateForm = this.formBuilder.group({
    email: [null, [Validators.email, Validators.required]],
    accessLevel: ["READ"],
  });

  public accessList: ReadonlyArray<ShareAccess> = [];
  public owner: string = "";
  public filteredOwners: Array<string> = [];
  public ownerSearchValue?: string;
  currentEmail: string | undefined = "";
  constructor(
    public activeModal: NgbActiveModal,
    private accessService: ShareAccessService,
    private formBuilder: FormBuilder,
    private userService: UserService
  ) {
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
        .grantAccess(
          this.type,
          this.id,
          this.validateForm.get("email")?.value,
          this.validateForm.get("accessLevel")?.value
        )
        .pipe(untilDestroyed(this))
        .subscribe(() => this.ngOnInit());
    }
  }

  public revokeAccess(userToRemove: string): void {
    this.accessService
      .revokeAccess(this.type, this.id, userToRemove)
      .pipe(untilDestroyed(this))
      .subscribe(() => this.ngOnInit());
  }
}
