import { TestBed } from "@angular/core/testing";
import { HttpClientTestingModule } from "@angular/common/http/testing";
import { DownloadService } from "./download.service";
import { DatasetService } from "../dataset/dataset.service";
import { FileSaverService } from "../file/file-saver.service";
import { NotificationService } from "../../../../common/service/notification/notification.service";
import { WorkflowPersistService } from "../../../../common/service/workflow-persist/workflow-persist.service";
import { of, throwError } from "rxjs";

describe("DownloadService", () => {
  let downloadService: DownloadService;
  let datasetServiceSpy: jasmine.SpyObj<DatasetService>;
  let fileSaverServiceSpy: jasmine.SpyObj<FileSaverService>;
  let notificationServiceSpy: jasmine.SpyObj<NotificationService>;

  beforeEach(() => {
    const datasetSpy = jasmine.createSpyObj("DatasetService", ["retrieveDatasetVersionSingleFile"]);
    const fileSaverSpy = jasmine.createSpyObj("FileSaverService", ["saveAs"]);
    const notificationSpy = jasmine.createSpyObj("NotificationService", ["info", "error"]);
    const workflowPersistSpy = jasmine.createSpyObj("WorkflowPersistService", ["getWorkflow"]);

    TestBed.configureTestingModule({
      imports: [HttpClientTestingModule],
      providers: [
        DownloadService,
        { provide: DatasetService, useValue: datasetSpy },
        { provide: FileSaverService, useValue: fileSaverSpy },
        { provide: NotificationService, useValue: notificationSpy },
        { provide: WorkflowPersistService, useValue: workflowPersistSpy },
      ],
    });

    downloadService = TestBed.inject(DownloadService);
    datasetServiceSpy = TestBed.inject(DatasetService) as jasmine.SpyObj<DatasetService>;
    fileSaverServiceSpy = TestBed.inject(FileSaverService) as jasmine.SpyObj<FileSaverService>;
    notificationServiceSpy = TestBed.inject(NotificationService) as jasmine.SpyObj<NotificationService>;
  });

  it("should download a single file successfully", done => {
    const filePath = "test/file.txt";
    const mockBlob = new Blob(["test content"], { type: "text/plain" });

    datasetServiceSpy.retrieveDatasetVersionSingleFile.and.returnValue(of(mockBlob));

    downloadService.downloadSingleFile(filePath).subscribe({
      next: blob => {
        expect(blob).toBe(mockBlob);
        expect(datasetServiceSpy.retrieveDatasetVersionSingleFile).toHaveBeenCalledWith(filePath);
        expect(fileSaverServiceSpy.saveAs).toHaveBeenCalledWith(mockBlob, "file.txt");
        expect(notificationServiceSpy.info).toHaveBeenCalledWith("File test/file.txt is downloading");
        done();
      },
      error: (error: unknown) => {
        fail("Should not have thrown an error");
      },
    });
  });

  it("should handle download failure correctly", done => {
    const filePath = "test/file.txt";
    const errorMessage = "Download failed";

    datasetServiceSpy.retrieveDatasetVersionSingleFile.and.returnValue(throwError(() => new Error(errorMessage)));

    downloadService.downloadSingleFile(filePath).subscribe({
      next: () => {
        fail("Should have thrown an error");
      },
      error: (error: unknown) => {
        expect(error).toBeTruthy();
        expect(datasetServiceSpy.retrieveDatasetVersionSingleFile).toHaveBeenCalledWith(filePath);
        expect(fileSaverServiceSpy.saveAs).not.toHaveBeenCalled();
        expect(notificationServiceSpy.error).toHaveBeenCalledWith("Error downloading file 'test/file.txt'");
        done();
      },
    });
  });
});
