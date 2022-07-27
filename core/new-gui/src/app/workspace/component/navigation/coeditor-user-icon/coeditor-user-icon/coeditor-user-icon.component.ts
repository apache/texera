import {Component, Input} from "@angular/core";
import {User} from "../../../../../common/type/user";

@Component({
  selector: "texera-coeditor-user-icon",
  templateUrl: "./coeditor-user-icon.component.html",
  styleUrls: ["./coeditor-user-icon.component.css"]
})
export class CoeditorUserIconComponent  {
  @Input() coeditor: User = {name: "", uid: -1};
}
