import { Injectable } from "@angular/core";
import { Observable } from "rxjs";
import { AppSettings } from "../../../common/app-setting";
import { HttpClient } from "@angular/common/http";
import html2canvas from "html2canvas";
import { WorkflowSnapshotEntry } from "../../type/workflow-snapshot-entry";


export const WORKFLOW_SNAPSHOT_API_BASE_URL = `${AppSettings.getApiEndpoint()}/snapshot`;
export const WORKFLOW_SNAPSHOT_UPLOAD_URL = `${WORKFLOW_SNAPSHOT_API_BASE_URL}/upload`;

@Injectable({
    providedIn: "root",
})
export class WorkflowSnapshotService {
    constructor(private http: HttpClient) {}

    /**
    * take a snapshot of the current workflow editor
    */
    public createSnapShotCanvas(): Promise<HTMLCanvasElement> {
        let doc = document.getElementById("texera-workflow-editor") || document.body;
        const { height, width } = doc.getBoundingClientRect();
        return html2canvas(doc, {
            allowTaint: true,
            useCORS: true,
            backgroundColor: "transparent",
            height: height * 0.6,
            y: height * 0.2,
            width: width * 0.7,
            x: width * 0.15,
        });
    }

    public uploadWorkflowSnapshot(snapshotBlob: Blob, wid: number | undefined): Observable<Response> {
        const formData: FormData = new FormData();
        formData.append("wid", wid?.toString() || "");
        formData.append("SnapshotBlob", snapshotBlob);
        return this.http.put<Response>(`${WORKFLOW_SNAPSHOT_UPLOAD_URL}`, formData);
    }

    public retrieveWorkflowSnapshot(sid: number): Observable<WorkflowSnapshotEntry> {
        return this.http.get<WorkflowSnapshotEntry>(`${WORKFLOW_SNAPSHOT_API_BASE_URL}/${sid}`)
    }
}
