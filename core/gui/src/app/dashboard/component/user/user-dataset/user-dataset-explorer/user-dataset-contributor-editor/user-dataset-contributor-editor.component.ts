import { Component, EventEmitter, OnInit, Output, Inject } from "@angular/core";
import { FormGroup } from "@angular/forms";
import { NZ_MODAL_DATA } from "ng-zorro-antd/modal";
import { NzModalRef } from "ng-zorro-antd/modal";
import { FormlyFieldConfig } from "@ngx-formly/core";

export interface ContributorData {
  id?: number;
  name: string;
  creator: boolean;
  role: string;
  email: string;
  affiliation: string;
}

@Component({
  selector: "texera-user-dataset-contributor-editor",
  templateUrl: "./user-dataset-contributor-editor.component.html",
})
export class UserDatasetContributorEditor implements OnInit {
  @Output() contributorChange = new EventEmitter<ContributorData>();

  contributorForm = new FormGroup({});
  model: ContributorData = {
    name: "",
    creator: false,
    role: "RESEARCHER",
    email: "",
    affiliation: "",
  };

  roles = ["RESEARCHER", "PRINCIPAL INVESTIGATOR", "PROJECT MEMBER", "OTHER"];

  fields: FormlyFieldConfig[] = [
    {
      key: "name",
      type: "input",
      templateOptions: {
        label: "Name",
        required: true,
        placeholder: "Enter name",
      },
    },
    {
      key: "creator",
      type: "checkbox",
      templateOptions: {
        label: "Creator",
        // Formly checkbox is a boolean switch by default
      },
    },
    {
      key: "role",
      type: "select",
      templateOptions: {
        label: "Role",
        options: this.roles.map(r => ({ label: r, value: r })),
      },
    },
    {
      key: "email",
      type: "input",
      templateOptions: {
        label: "Email",
        type: "email",
        placeholder: "Enter email",
        pattern: /^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,4}$/,
      },
      validation: {
        messages: {
          pattern: "Please enter a valid email address",
        },
      },
    },
    {
      key: "affiliation",
      type: "input",
      templateOptions: {
        label: "Affiliation",
        placeholder: "Enter affiliation",
      },
    },
  ];

  constructor(
    @Inject(NZ_MODAL_DATA) public contributorData: ContributorData,
    private modalRef: NzModalRef
  ) {}

  ngOnInit() {
    if (this.contributorData) {
      this.model = { ...this.contributorData };
    }
  }

  submit() {
    if (this.contributorForm.invalid) {
      return;
    }
    const data: ContributorData = {
      id: this.contributorData?.id,
      ...this.model,
    };
    this.contributorChange.emit(data);
    this.modalRef.close(data);
  }

  cancel() {
    this.modalRef.close();
  }
}
