import { ComponentFixture, TestBed } from "@angular/core/testing";
import { HttpClientTestingModule } from "@angular/common/http/testing";
import { ListItemComponent } from "./list-item.component";
import { SearchService } from "src/app/dashboard/service/user/search.service";
import { NzModalModule } from "ng-zorro-antd/modal";
import { NzModalService } from "ng-zorro-antd/modal";
import { BrowserAnimationsModule } from "@angular/platform-browser/animations";
import { DashboardEntry } from "../../../type/dashboard-entry";

describe("ListItemComponent", () => {
  let component: ListItemComponent;
  let fixture: ComponentFixture<ListItemComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [HttpClientTestingModule, NzModalModule, BrowserAnimationsModule],
      declarations: [ListItemComponent],
      providers: [SearchService, NzModalService],
    }).compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(ListItemComponent);
    component = fixture.componentInstance;

    // Create a mock DashboardEntry
    const mockEntry = new DashboardEntry({
      file: {
        fid: 1,
        name: "Test File",
        path: "/test/path", // Add this line
        description: "Test Description",
        uploadTime: Date.now(),
        ownerUid: 1,
        size: 1536, // 1.5 KB
      },
      accessLevel: "read",
      ownerEmail: "test@example.com",
    });

    component.entry = mockEntry;

    fixture.detectChanges();
  });

  describe("formatSize", () => {
    it("should correctly format a valid file size", () => {
      const result = component.formatSize(component.entry.size);
      expect(result).toBe("1.50 KB");
    });

    it("should return \"0 Bytes\" for undefined or non-positive input", () => {
      expect(component.formatSize(undefined)).toBe("0 Bytes");
      expect(component.formatSize(-100)).toBe("0 Bytes");
      expect(component.formatSize(0)).toBe("0 Bytes");
    });

    it("should correctly format large file sizes", () => {
      const largeSizeInBytes = 1073741824; // 1 GB
      const result = component.formatSize(largeSizeInBytes);
      expect(result).toBe("1.00 GB");
    });

    it("should handle decimal places correctly", () => {
      const result = component.formatSize(1500); // 1.46 KB
      expect(result).toBe("1.46 KB");
    });

    it("should successfully format a typical file size", () => {
      const typicalSizeInBytes = 2097152; // 2 MB
      const result = component.formatSize(typicalSizeInBytes);
      expect(result).toBe("2.00 MB");
    });

    it("should handle extremely large file sizes without failure", () => {
      const extremelyLargeSizeInBytes = Number.MAX_SAFE_INTEGER; // Largest safe integer in JavaScript
      const result = component.formatSize(extremelyLargeSizeInBytes);
      expect(result).not.toBe("0 Bytes");
      expect(result).toContain("TB"); // Should be in terabytes
    });
  });
});
