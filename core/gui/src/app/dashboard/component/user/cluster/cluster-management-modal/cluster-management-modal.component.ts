import { Component, Input, Output, EventEmitter } from "@angular/core";
import { FormBuilder, FormGroup, Validators } from "@angular/forms";

@Component({
  selector: "app-cluster-management-modal",
  templateUrl: "./cluster-management-modal.component.html",
  styleUrls: ["./cluster-management-modal.component.scss"],
})
export class ClusterManagementModalComponent {
  @Input() isVisible: boolean = false;
  @Output() closeModal = new EventEmitter<void>();
  @Output() submitClusterEvent = new EventEmitter<FormGroup>();
  clusterForm!: FormGroup;

  machineOptions = [
    { value: "t2.micro", label: "t2.micro - 1 CPU, 1 GB RAM, $0.0116/hour" },
    { value: "t3.large", label: "t3.large - 2 CPUs, 8 GB RAM, $0.0832/hour" },
    {
      value: "t3.xlarge",
      label: "t3.xlarge - 4 CPUs, 16 GB RAM, $0.1664/hour",
    },
    {
      value: "t3.2xlarge",
      label: "t3.2xlarge - 8 CPUs, 32 GB RAM, $0.3328/hour",
    },
  ];

  machineNumbers = [1, 2, 3, 4, 5, 6, 7, 8];

  constructor(private fb: FormBuilder) {}

  ngOnInit(): void {
    this.clusterForm = this.fb.group({
      Name: [null, [Validators.required]],
      machineType: [null, [Validators.required]],
      numberOfMachines: [null, [Validators.required]],
    });
  }

  closeClusterManagementModal() {
    this.closeModal.emit();
  }

  submitCluster() {
    if (this.clusterForm.valid) {
      this.submitClusterEvent.emit(this.clusterForm);
    } else {
      Object.values(this.clusterForm.controls).forEach(control => {
        if (control.invalid) {
          control.markAsDirty();
          control.updateValueAndValidity({ onlySelf: true });
        }
      });
    }
  }
}
