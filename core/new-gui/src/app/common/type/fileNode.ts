export interface FileNode {
  path: string;
  isFile: boolean;
  children?: FileNode[];
}
