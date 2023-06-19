import { Injectable } from "@angular/core";
import * as FileSaver from "file-saver";

@Injectable({
  providedIn: "root",
})
export class FileSaverService {
  constructor() {}

  saveAs(data: Blob | string, filename?: string, options?: FileSaver.FileSaverOptions): void {
    FileSaver.saveAs(data, filename, options);
  }
}
