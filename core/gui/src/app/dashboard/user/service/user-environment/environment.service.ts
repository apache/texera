import { Injectable } from "@angular/core";

import { HttpClient } from "@angular/common/http";
import next from "ajv/dist/vocabularies/next";
import { Observable, of, throwError } from "rxjs";
import { catchError, filter, map } from "rxjs/operators";
import { AppSettings } from "../../../../common/app-setting";
import { DatasetOfEnvironment, DatasetOfEnvironmentDetails, Environment } from "../../../../common/type/environment";
import { DashboardDataset } from "../../type/dashboard-dataset.interface";
import { DATASET_BASE_URL, DATASET_VERSION_BASE_URL } from "../user-dataset/dataset.service";
import {
  DatasetVersionFileTreeNode,
  EnvironmentDatasetFileNodes,
  parseFileNodesToTreeNodes,
} from "../../../../common/type/datasetVersionFileTree";
import { FileNode } from "../../../../common/type/fileNode";

export const ENVIRONMENT_BASE_URL = "environment";
export const ENVIRONMENT_CREATE_URL = ENVIRONMENT_BASE_URL + "/create";
export const ENVIRONMENT_DELETE_URL = ENVIRONMENT_BASE_URL + "/delete";
export const ENVIRONMENT_GET_DATASETS_FILELIST = "files";
export const ENVIRONMENT_GET_DATASETS_FILENODE_LIST = "fileNodes";
export const ENVIRONMENT_DATASET_RETRIEVAL_URL = "dataset";
export const ENVIRONMENT_DATASET_DETAILS_RETRIEVAL_URL = ENVIRONMENT_DATASET_RETRIEVAL_URL + "/details";

export const ENVIRONMENT_DATASET_ADD_URL = ENVIRONMENT_DATASET_RETRIEVAL_URL + "/add";

export const ENVIRONMENT_DATASET_REMOVE_URL = ENVIRONMENT_DATASET_RETRIEVAL_URL + "/remove";
export const ENVIRONMENT_DATASET_VERSION_UPDATE = "updateVersion";

@Injectable({
  providedIn: "root",
})
export class EnvironmentService {
  constructor(private http: HttpClient) {}

  addDatasetToEnvironment(eid: number, did: number): Observable<Response> {
    return this.http.post<Response>(
      `${AppSettings.getApiEndpoint()}/${ENVIRONMENT_BASE_URL}/${eid}/${ENVIRONMENT_DATASET_ADD_URL}`,
      {
        did: did,
      }
    );
  }

  removeDatasetFromEnvironment(eid: number, did: number): Observable<Response> {
    return this.http.post<Response>(
      `${AppSettings.getApiEndpoint()}/${ENVIRONMENT_BASE_URL}/${eid}/${ENVIRONMENT_DATASET_REMOVE_URL}`,
      {
        did: did,
      }
    );
  }

  updateDatasetVersionInEnvironment(eid: number, did: number, dvid: number): Observable<Response> {
    return this.http.post<Response>(
      `${AppSettings.getApiEndpoint()}/${ENVIRONMENT_BASE_URL}/${eid}/dataset/${did}/${ENVIRONMENT_DATASET_VERSION_UPDATE}`,
      {
        dvid: dvid,
      }
    );
  }

  retrieveDatasetsOfEnvironment(eid: number): Observable<DatasetOfEnvironment[]> {
    return this.http.get<DatasetOfEnvironment[]>(
      `${AppSettings.getApiEndpoint()}/${ENVIRONMENT_BASE_URL}/${eid}/${ENVIRONMENT_DATASET_RETRIEVAL_URL}`
    );
  }

  retrieveDatasetsOfEnvironmentDetails(eid: number): Observable<DatasetOfEnvironmentDetails[]> {
    return this.http.get<DatasetOfEnvironmentDetails[]>(
      `${AppSettings.getApiEndpoint()}/${ENVIRONMENT_BASE_URL}/${eid}/${ENVIRONMENT_DATASET_DETAILS_RETRIEVAL_URL}`
    );
  }

  // Delete: Remove an environment by its index (eid)
  deleteEnvironments(eids: number[]): Observable<Response> {
    return this.http.post<Response>(`${AppSettings.getApiEndpoint()}/${ENVIRONMENT_DELETE_URL}`, {
      eids: eids,
    });
  }

  public getDatasetsFileNodeList(eid: number): Observable<DatasetVersionFileTreeNode[]> {
    return this.http
      .get<
        {
          datasetName: string;
          fileNodes: FileNode[];
        }[]
      >(`${AppSettings.getApiEndpoint()}/${ENVIRONMENT_BASE_URL}/${eid}/${ENVIRONMENT_GET_DATASETS_FILENODE_LIST}`)
      .pipe(
        map(response => {
          let nodes: DatasetVersionFileTreeNode[] = [];
          response.forEach(entry => {
            const datasetDirectoryNode: DatasetVersionFileTreeNode = {
              name: entry.datasetName,
              type: "directory",
              parentDir: "/",
              children: parseFileNodesToTreeNodes(entry.fileNodes, entry.datasetName),
            };
            nodes.push(datasetDirectoryNode);
          });
          return nodes;
        })
      );
  }

  public getDatasetsFileList(eid: number, query: String): Observable<ReadonlyArray<string>> {
    if (query == "") {
      return this.http.get<ReadonlyArray<string>>(
        `${AppSettings.getApiEndpoint()}/${ENVIRONMENT_BASE_URL}/${eid}/${ENVIRONMENT_GET_DATASETS_FILELIST}/`
      );
    }
    return this.http.get<ReadonlyArray<string>>(
      `${AppSettings.getApiEndpoint()}/${ENVIRONMENT_BASE_URL}/${eid}/${ENVIRONMENT_GET_DATASETS_FILELIST}/${query}`
    );
  }
}
