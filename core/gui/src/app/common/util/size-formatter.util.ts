const BYTES_PER_UNIT = 1024;
const SIZE_UNITS = ["B", "KB", "MB", "GB", "TB"];

export const formatSize = (bytes?: number): string => {
  if (bytes === undefined || bytes <= 0) return "0 B";

  const unitIndex = Math.min(Math.floor(Math.log(bytes) / Math.log(BYTES_PER_UNIT)), SIZE_UNITS.length - 1);
  const size = bytes / Math.pow(BYTES_PER_UNIT, unitIndex);

  return `${size.toFixed(2)} ${SIZE_UNITS[unitIndex]}`;
};
