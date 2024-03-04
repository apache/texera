import {Dataset, DatasetVersion} from "./dataset";

export interface Environment {
  eid: number | undefined;
  uid: number | undefined;
  name: string;
  description: string;
  creationTime: number | undefined;
}

export interface DatasetOfEnvironment {
  did: number;
  eid: number;
  dvid: number;
}

export interface DatasetOfEnvironmentDetails {
  dataset: Dataset;
  version: DatasetVersion;
}
