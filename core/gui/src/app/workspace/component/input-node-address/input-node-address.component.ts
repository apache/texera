import { Component, OnInit, OnDestroy } from "@angular/core";
import { FieldType, FieldTypeConfig } from "@ngx-formly/core";
import { Subject, Subscription } from "rxjs";
import { WorkflowWebsocketService } from "../../service/workflow-websocket/workflow-websocket.service";
import { takeUntil } from "rxjs/operators";

@Component({
  selector: "texera-input-node-address",
  templateUrl: "./input-node-address.component.html",
  styleUrls: ["./input-node-address.component.scss"],
})
export class InputNodeAddressComponent extends FieldType<FieldTypeConfig> implements OnInit, OnDestroy {
  public nodeAddresses: string[] = [];

  private wsSubscription!: Subscription;

  private componentDestroy = new Subject<void>();

  constructor(private workflowWebsocketService: WorkflowWebsocketService) {
    super();
  }

  ngOnInit(): void {
    this.nodeAddresses = this.workflowWebsocketService.workerAddresses;

    this.wsSubscription = this.workflowWebsocketService
      .websocketEvent()
      .pipe(takeUntil(this.componentDestroy))
      .subscribe(event => {
        if (event.type === "ClusterStatusUpdateEvent") {
          this.nodeAddresses = event.addresses;
        }
      });
  }

  ngOnDestroy(): void {
    this.componentDestroy.next();
    this.componentDestroy.complete();
  }
}
