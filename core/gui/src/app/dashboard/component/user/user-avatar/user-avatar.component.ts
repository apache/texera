import { Component, Input } from "@angular/core";
@Component({
  selector: "texera-user-avatar",
  templateUrl: "./user-avatar.component.html",
  styleUrls: ["./user-avatar.component.scss"],
})

/**
 * UserAvatarComponent is used to show the avatar of a user
 * The avatar of a Google user will be its Google profile picture
 * The avatar of a normal user will be a default one with the initial
 */
export class UserAvatarComponent {
  @Input() googleAvatar?: string;
  @Input() userName?: string;
  @Input() userColor?: string;
  @Input() isOwner: Boolean = false;

  /**
   * abbreviates the name under 5 chars
   * @param userName
   */
  public abbreviate(userName: string): string {
    if (userName.length <= 5) {
      return userName;
    } else {
      return userName.slice(0, 5);
    }
  }
}
