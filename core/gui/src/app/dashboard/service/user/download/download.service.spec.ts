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
    const datasetSpy = jasmine.createSpyObj("DatasetService", [
      "retrieveDatasetVersionSingleFile",
      "retrieveDatasetZip", // Add this method to the spy
    ]);
    const fileSaverSpy = jasmine.createSpyObj("FileSaverService", ["saveAs"]);
    const notificationSpy = jasmine.createSpyObj("NotificationService", ["info", "success", "error"]);
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

  it("should download a single file successfully", (done: DoneFn) => {
    const filePath = "test/file.txt";
    const mockBlob = new Blob(["test content"], { type: "text/plain" });

    datasetServiceSpy.retrieveDatasetVersionSingleFile.and.returnValue(of(mockBlob));

    downloadService.downloadSingleFile(filePath).subscribe({
      next: blob => {
        expect(blob).toBe(mockBlob);
        expect(notificationServiceSpy.info).toHaveBeenCalledWith("Starting to download file test/file.txt");
        expect(datasetServiceSpy.retrieveDatasetVersionSingleFile).toHaveBeenCalledWith(filePath);
        expect(fileSaverServiceSpy.saveAs).toHaveBeenCalledWith(mockBlob, "file.txt");
        expect(notificationServiceSpy.success).toHaveBeenCalledWith("File test/file.txt has been downloaded");
        done();
      },
      error: (error: unknown) => {
        fail("Should not have thrown an error: " + error);
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
        expect(notificationServiceSpy.info).toHaveBeenCalledWith("Starting to download file test/file.txt");
        expect(datasetServiceSpy.retrieveDatasetVersionSingleFile).toHaveBeenCalledWith(filePath);
        expect(fileSaverServiceSpy.saveAs).not.toHaveBeenCalled();
        expect(notificationServiceSpy.error).toHaveBeenCalledWith("Error downloading file 'test/file.txt'");
        done();
      },
    });
  });

  it("should download a dataset successfully", done => {
    const datasetId = 1;
    const datasetName = "TestDataset";
    const mockBlob = new Blob(["dataset content"], { type: "application/zip" });

    datasetServiceSpy.retrieveDatasetZip.and.returnValue(of(mockBlob));

    downloadService.downloadDataset(datasetId, datasetName).subscribe({
      next: blob => {
        expect(blob).toBe(mockBlob);
        expect(notificationServiceSpy.info).toHaveBeenCalledWith(
          "Starting to download the latest version of the dataset as ZIP"
        );
        expect(datasetServiceSpy.retrieveDatasetZip).toHaveBeenCalledWith({ did: datasetId });
        expect(fileSaverServiceSpy.saveAs).toHaveBeenCalledWith(mockBlob, "TestDataset.zip");
        expect(notificationServiceSpy.success).toHaveBeenCalledWith(
          "The latest version of the dataset has been downloaded as ZIP"
        );
        done();
      },
      error: (error: unknown) => {
        fail("Should not have thrown an error");
      },
    });
  });

  it("should handle dataset download failure correctly", done => {
    const datasetId = 1;
    const datasetName = "TestDataset";
    const errorMessage = "Dataset download failed";

    datasetServiceSpy.retrieveDatasetZip.and.returnValue(throwError(() => new Error(errorMessage)));

    downloadService.downloadDataset(datasetId, datasetName).subscribe({
      next: () => {
        fail("Should have thrown an error");
      },
      error: (error: unknown) => {
        expect(error).toBeTruthy();
        expect(notificationServiceSpy.info).toHaveBeenCalledWith(
          "Starting to download the latest version of the dataset as ZIP"
        );
        expect(datasetServiceSpy.retrieveDatasetZip).toHaveBeenCalledWith({ did: datasetId });
        expect(fileSaverServiceSpy.saveAs).not.toHaveBeenCalled();
        expect(notificationServiceSpy.error).toHaveBeenCalledWith(
          "Error downloading the latest version of the dataset as ZIP"
        );
        done();
      },
    });
  });

  it("should download a dataset version successfully", done => {
    const datasetId = 1;
    const datasetVersionId = 1;
    const datasetName = "TestDataset";
    const versionName = "v1.0";
    const mockBlob = new Blob(["version content"], { type: "application/zip" });

    datasetServiceSpy.retrieveDatasetZip.and.returnValue(of(mockBlob));

    downloadService.downloadDatasetVersion(datasetId, datasetVersionId, datasetName, versionName).subscribe({
      next: blob => {
        expect(blob).toBe(mockBlob);
        expect(notificationServiceSpy.info).toHaveBeenCalledWith("Starting to download version v1.0 as ZIP");
        expect(datasetServiceSpy.retrieveDatasetZip).toHaveBeenCalledWith({ did: datasetId, dvid: datasetVersionId });
        expect(fileSaverServiceSpy.saveAs).toHaveBeenCalledWith(mockBlob, "TestDataset-v1.0.zip");
        expect(notificationServiceSpy.success).toHaveBeenCalledWith("Version v1.0 has been downloaded as ZIP");
        done();
      },
      error: (error: unknown) => {
        fail("Should not have thrown an error");
      },
    });
  });

  it("should handle dataset version download failure correctly", done => {
    const datasetId = 1;
    const datasetVersionId = 1;
    const datasetName = "TestDataset";
    const versionName = "v1.0";
    const errorMessage = "Dataset version download failed";

    datasetServiceSpy.retrieveDatasetZip.and.returnValue(throwError(() => new Error(errorMessage)));

    downloadService.downloadDatasetVersion(datasetId, datasetVersionId, datasetName, versionName).subscribe({
      next: () => {
        fail("Should have thrown an error");
      },
      error: (error: unknown) => {
        expect(error).toBeTruthy();
        expect(notificationServiceSpy.info).toHaveBeenCalledWith("Starting to download version v1.0 as ZIP");
        expect(datasetServiceSpy.retrieveDatasetZip).toHaveBeenCalledWith({ did: datasetId, dvid: datasetVersionId });
        expect(fileSaverServiceSpy.saveAs).not.toHaveBeenCalled();
        expect(notificationServiceSpy.error).toHaveBeenCalledWith("Error downloading version 'v1.0' as ZIP");
        done();
      },
    });
  });

  it("should download workflows as ZIP successfully", done => {
    const workflowEntries = [
      { id: 1, name: "Workflow1" },
      { id: 2, name: "Workflow2" },
    ];
    const mockBlob = new Blob(["zip content"], { type: "application/zip" });

    spyOn(downloadService as any, "createWorkflowsZip").and.returnValue(of(mockBlob));

    downloadService.downloadWorkflowsAsZip(workflowEntries).subscribe({
      next: blob => {
        expect(blob).toBe(mockBlob);
        expect(notificationServiceSpy.info).toHaveBeenCalledWith("Starting to download workflows as ZIP");
        expect((downloadService as any).createWorkflowsZip).toHaveBeenCalledWith(workflowEntries);
        expect(fileSaverServiceSpy.saveAs).toHaveBeenCalledWith(
          mockBlob,
          jasmine.stringMatching(/^workflowExports-.*\.zip$/)
        );
        expect(notificationServiceSpy.success).toHaveBeenCalledWith("Workflows have been downloaded as ZIP");
        done();
      },
      error: (error: unknown) => {
        fail("Should not have thrown an error");
      },
    });
  });

  it("should handle workflows ZIP download failure correctly", done => {
    const workflowEntries = [
      { id: 1, name: "Workflow1" },
      { id: 2, name: "Workflow2" },
    ];
    const errorMessage = "Workflows ZIP download failed";

    spyOn(downloadService as any, "createWorkflowsZip").and.returnValue(throwError(() => new Error(errorMessage)));

    downloadService.downloadWorkflowsAsZip(workflowEntries).subscribe({
      next: () => {
        fail("Should have thrown an error");
      },
      error: (error: unknown) => {
        expect(error).toBeTruthy();
        expect(notificationServiceSpy.info).toHaveBeenCalledWith("Starting to download workflows as ZIP");
        expect((downloadService as any).createWorkflowsZip).toHaveBeenCalledWith(workflowEntries);
        expect(fileSaverServiceSpy.saveAs).not.toHaveBeenCalled();
        expect(notificationServiceSpy.error).toHaveBeenCalledWith("Error downloading workflows as ZIP");
        done();
      },
    });
  });
});
