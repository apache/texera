import { HttpClient } from "@angular/common/http";
import { Injectable } from "@angular/core";
import { GuiConfig } from "../type/gui-config";
import { Observable, tap, map, catchError } from "rxjs";
import { environment } from "../../../environments/environment";
import { throwError } from "rxjs";

@Injectable({ providedIn: "root" })
export class GuiConfigService {
  private config!: GuiConfig;

  constructor(private http: HttpClient) {}

  load(): Observable<void> {
    return this.http.get<GuiConfig>("/api/gui/config").pipe(
      tap(config => {
        this.config = config;
        console.log("GUI configuration loaded successfully from backend");
      }),
      map(() => void 0),
      catchError((error: unknown) => {
        console.error("Failed to load GUI configuration:", error);
        return throwError(() => new Error(`Failed to load GUI configuration from backend: ${error}`));
      })
    );
  }

  get env(): GuiConfig {
    if (!this.config) {
      throw new Error("GUI configuration not loaded yet. Make sure load() is called during app initialization");
    }
    return this.config;
  }
}
