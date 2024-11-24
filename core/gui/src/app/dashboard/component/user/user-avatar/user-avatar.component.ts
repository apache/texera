import { Component, Input } from "@angular/core";
import { UserService } from "../../../../common/service/user/user.service";
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

  constructor(private userService: UserService) {}

  get avatarUrl(): string {
    if (!this.googleAvatar) return "";

    if (this.userService.hasAvatar(this.googleAvatar)) {
      return this.userService.getAvatar(this.googleAvatar)!;
    }

    const url = `https://lh3.googleusercontent.com/a/${this.googleAvatar}`;
    this.userService.setAvatar(this.googleAvatar, url);
    return url;
  }

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
