import { TestBed } from "@angular/core/testing";
import { HttpClientTestingModule } from "@angular/common/http/testing";
import { UserDatasetFileRendererComponent } from "./user-dataset-file-renderer.component";
import { DatasetService } from "../../../../../service/user/dataset/dataset.service";
import { NotificationService } from "../../../../../../common/service/notification/notification.service";
import { DomSanitizer } from "@angular/platform-browser";

describe("UserDatasetFileRendererComponent", () => {
  let component: UserDatasetFileRendererComponent;

  beforeEach(() => {
    TestBed.configureTestingModule({
      imports: [HttpClientTestingModule],
      declarations: [UserDatasetFileRendererComponent],
      providers: [
        DatasetService,
        NotificationService,
        { provide: DomSanitizer, useValue: jasmine.createSpyObj("DomSanitizer", ["bypassSecurityTrustUrl"]) },
      ],
    });
    const fixture = TestBed.createComponent(UserDatasetFileRendererComponent);
    component = fixture.componentInstance;
  });

  it("should return true for supported MIME type", () => {
    const supportedMimeType = "image/jpeg"; // Example of a supported MIME type
    const result = component.isPreviewSupported(supportedMimeType);
    expect(result).toBeTrue();
  });

  it("should return false for unsupported MIME type", () => {
    const unsupportedMimeType = "application/unknown"; // Example of an unsupported MIME type
    const result = component.isPreviewSupported(unsupportedMimeType);
    expect(result).toBeFalse();
  });
});
