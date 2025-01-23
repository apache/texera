import { DatasetFileNode } from "./datasetVersionFileTree";

export interface DatasetVersion {
  dvid: number | undefined;
  did: number;
  creatorUid: number;
  name: string;
  versionHash: string | undefined;
  creationTime: number | undefined;
  fileNodes: DatasetFileNode[] | undefined;
}

export interface Dataset {
  did: number | undefined;
  ownerUid: number | undefined;
  name: string;
  isPublic: number;
  storagePath: string | undefined;
  description: string;
  creationTime: number | undefined;
  versionHierarchy: DatasetVersion[] | undefined;
}
