import { formatSize } from "./size-formatter.util";

describe("formatSize", () => {
  it("should correctly format a valid file size", () => {
    const result = formatSize(1536);
    expect(result).toBe("1.50 KB");
  });

  it("should return \"0 Bytes\" for undefined or non-positive input", () => {
    expect(formatSize(undefined)).toBe("0 B");
    expect(formatSize(-100)).toBe("0 B");
    expect(formatSize(0)).toBe("0 B");
  });

  it("should correctly format large file sizes", () => {
    const largeSizeInBytes = 1073741824;
    const result = formatSize(largeSizeInBytes);
    expect(result).toBe("1.00 GB");
  });

  it("should handle decimal places correctly", () => {
    const result = formatSize(1500);
    expect(result).toBe("1.46 KB");
  });

  it("should successfully format a typical file size", () => {
    const typicalSizeInBytes = 2097152;
    const result = formatSize(typicalSizeInBytes);
    expect(result).toBe("2.00 MB");
  });

  it("should handle extremely large file sizes without failure", () => {
    const extremelyLargeSizeInBytes = Number.MAX_SAFE_INTEGER;
    const result = formatSize(extremelyLargeSizeInBytes);
    expect(result).not.toBe("0 B");
    expect(result).toContain("TB");
  });
});
