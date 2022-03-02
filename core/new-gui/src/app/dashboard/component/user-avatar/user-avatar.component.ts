import { GooglePeopleApiResponse } from "../../type/google-api-response";
import { Component, OnInit, Input } from "@angular/core";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { HttpClient } from "@angular/common/http";
import { environment } from "../../../../environments/environment";

@UntilDestroy()
@Component({
  selector: "texera-user-avatar",
  templateUrl: "./user-avatar.component.html",
  styleUrls: ["./user-avatar.component.scss"],
})

/**
 * UserAvatarComponent is used to show the avatar of a user
 * The avatar of a Google user will be its Google profile picture
 * The avatar of a normal user will be a default one with the initial
 *
 * Use Google People API to retrieve google user's profile picture
 * Check https://developers.google.com/people/api/rest/v1/people/get for more details of the api usage
 *
 * @author Zhen Guan
 */
export class UserAvatarComponent implements OnInit {
  public googleUserAvatarSrc: string = "";
  private publicKey = environment.google.publicKey;

  constructor(private http: HttpClient) {}

  @Input() googleId?: string;
  @Input() userName?: string;

  ngOnInit(): void {
    if (!this.googleId && !this.userName) {
      throw new Error("google Id or user name should be provided");
    } else if (this.googleId) {
      this.userName = "";
      // get the avatar of the google user
      const googlePeopleAPIUrl = `https://people.googleapis.com/v1/people/${this.googleId}?personFields=names%2Cphotos&key=${this.publicKey}`;
      this.http
        .get<GooglePeopleApiResponse>(googlePeopleAPIUrl)
        .pipe(untilDestroyed(this))
        .subscribe(res => {
          this.googleUserAvatarSrc = res.photos[0].url;
        });
    } else {
      const r = Math.floor(Math.random() * 255);
      const g = Math.floor(Math.random() * 255);
      const b = Math.floor(Math.random() * 255);
      const avatar = document.getElementById("texera-user-avatar");
      if (avatar) {
        avatar.style.backgroundColor = "rgba(" + r + "," + g + "," + b + ",0.8)";
      }
    }
  }

  /**
   * abbreviates the name under 5 chars
   * @param userName
   */
  public abbreviate(userName: string): string {
    if (userName.length <= 5) {
      return userName;
    } else {
      return this.getUserInitial(userName).slice(0, 5);
    }
  }

  public getUserInitial(userName: string): string {
    return userName + "he";
  }
}
