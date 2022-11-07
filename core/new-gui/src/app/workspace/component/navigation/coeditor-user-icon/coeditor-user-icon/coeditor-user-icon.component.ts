import { Component, Input } from "@angular/core";
import { Coeditor } from "../../../../../common/type/user";
import { CoeditorPresenceService } from "../../../../service/workflow-graph/model/coeditor-presence.service";

/**
 * CoeditorUserIconComponent is the user icon of a co-editor.
 *
 * It is also the entry for shadowing mode.
 */

@Component({
  selector: "texera-coeditor-user-icon",
  templateUrl: "./coeditor-user-icon.component.html",
  styleUrls: ["./coeditor-user-icon.component.css"],
})
export class CoeditorUserIconComponent {
  @Input() coeditor: Coeditor = { name: "", uid: -1, clientId: "0" };

  constructor(public coeditorPresenceService: CoeditorPresenceService) {}

  public shadowCoeditor() {
    this.coeditorPresenceService.shadowCoeditor(this.coeditor);
  }

  stopShadowing() {
    this.coeditorPresenceService.stopShadowing();
  }
}
