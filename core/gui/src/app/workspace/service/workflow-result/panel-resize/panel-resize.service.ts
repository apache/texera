import { Injectable } from "@angular/core";
import { BehaviorSubject } from "rxjs";

@Injectable({
  providedIn: "root",
})
export class PanelResizeService {
  private panelSizeSource = new BehaviorSubject<{ width: number; height: number }>({ width: 800, height: 300 });
  currentSize = this.panelSizeSource.asObservable();
  public pageSize = 1 + Math.floor((300 - 200) / 35);

  changePanelSize(width: number, height: number) {
    this.panelSizeSource.next({ width, height });
  }
}
