// user given filePath is /ownerEmail/datasetName/versionName/fileRelativePath
// e.g. /bob@texera.com/twitterDataset/v1/california/irvine/tw1.csv
export interface DatasetFile {
  ownerEmail: string;
  datasetName: string;
  versionName: string;
  fileRelativePath: string;
}

/**
 * Parses a file path string to a DatasetFile interface.
 * @param filePath - The file path string to parse.
 * @returns The parsed DatasetFile object.
 */
export function parseFilePathToDatasetFile(filePath: string): DatasetFile {
  const parts = filePath.split("/").filter(part => part.length > 0);

  if (parts.length < 4) {
    throw new Error("Invalid file path format");
  }

  const [ownerEmail, datasetName, versionName, ...fileRelativePathParts] = parts;
  const fileRelativePath = fileRelativePathParts.join("/");

  return {
    ownerEmail,
    datasetName,
    versionName,
    fileRelativePath,
  };
}

/**
 * Converts a DatasetFile object to a file path string.
 * @param datasetFile - The DatasetFile object to convert.
 * @returns The file path string.
 */
export function parseDatasetFileToFilePath(datasetFile: DatasetFile): string {
  const { ownerEmail, datasetName, versionName, fileRelativePath } = datasetFile;
  return `/${ownerEmail}/${datasetName}/${versionName}/${fileRelativePath}`;
}
