import { Component, Input, OnInit } from "@angular/core";
import { NgbActiveModal } from "@ng-bootstrap/ng-bootstrap";
import { FormBuilder, Validators } from "@angular/forms";
import { ShareAccessService } from "../../service/share-access/share-access.service";
import { ShareAccess } from "../../type/share-access.interface";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { UserService } from "../../../../common/service/user/user.service";
import { PublicProjectService } from "../../service/public-project/public-project.service";

@UntilDestroy()
@Component({
  templateUrl: "share-access.component.html",
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
  isAdmin: boolean = false;
  isPublic: boolean = false;
  constructor(
    public activeModal: NgbActiveModal,
    private accessService: ShareAccessService,
    private formBuilder: FormBuilder,
    private userService: UserService,
    private publicProjectService: PublicProjectService
  ) {
    this.currentEmail = this.userService.getCurrentUser()?.email;
    this.isAdmin = this.userService.isAdmin();
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
    this.publicProjectService
      .getType(this.id)
      .pipe(untilDestroyed(this))
      .subscribe(type => {
        this.isPublic = type === "Public";
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
      .subscribe(() => {
        if (this.currentEmail === userToRemove) {
          this.activeModal.close();
        }
        this.ngOnInit();
      });
  }

  public visibilityChange(): void {
    if (this.isPublic) {
      this.publicProjectService
        .makePrivate(this.id)
        .pipe(untilDestroyed(this))
        .subscribe(() => this.ngOnInit());
    } else {
      this.publicProjectService
        .makePublic(this.id)
        .pipe(untilDestroyed(this))
        .subscribe(() => this.ngOnInit());
    }
  }
}
