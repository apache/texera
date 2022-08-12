import {Component, Input} from "@angular/core";
import {User} from "../../../../../common/type/user";
import {CoeditorPresenceService} from "../../../../service/workflow-graph/model/coeditor-presence.service";

@Component({
  selector: "texera-coeditor-user-icon",
  templateUrl: "./coeditor-user-icon.component.html",
  styleUrls: ["./coeditor-user-icon.component.css"]
})
export class CoeditorUserIconComponent  {

  constructor(public coeditorPresenceService: CoeditorPresenceService,) {

  }
  @Input() coeditor: User = {name: "", uid: -1};

  public shadowCoeditor() {
    this.coeditorPresenceService.shadowCoeditor(this.coeditor);
  }

  stopShadowing() {
    this.coeditorPresenceService.stopShadowing();
  }
}
