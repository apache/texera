import { Injectable } from "@angular/core";
import { Dataset, DatasetVersion } from "../../../../common/type/dataset";
import { DashboardDataset } from "../../../type/dashboard-dataset.interface";

import { Observable, throwError } from "rxjs";
import { FileUploadItem } from "../../../type/dashboard-file.interface";
import { RepositoriesService, 
  RepositoryCreation, 
  CommitsService,
	Commit,
  RefsService,
  ObjectsService, 
  PathList} from "lakefs"
import { S3Client, 
  PutObjectCommand, 
  CreateMultipartUploadCommand, 
  UploadPartCommand, 
  CompleteMultipartUploadCommand, 
  AbortMultipartUploadCommand} from "@aws-sdk/client-s3"
import { defaultEnvironment } from "src/environments/environment.default";
import { DatasetFileNode } from "src/app/common/type/datasetVersionFileTree";

const LAKEFS_ACCESS_KEY = "AKIAIOSFOLKFSSAMPLES"
const LAKEFS_SECRET_KEY = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"

const CHUNK_SIZE = 5 * 1024 * 1024; // 5MB per part (AWS & LakeFS minimum)

@Injectable({
    providedIn: "root",
  })
export class LakefsDatasetService {
  private s3Client: S3Client;

  constructor(private repositoriesService: RepositoriesService, 
    private commitsService: CommitsService,
    private refsService: RefsService,
    private objectsService: ObjectsService
  ) {
    this.repositoriesService.configuration.username = LAKEFS_ACCESS_KEY;
    this.repositoriesService.configuration.password = LAKEFS_SECRET_KEY;
    this.refsService.configuration.username = LAKEFS_ACCESS_KEY;
    this.refsService.configuration.password = LAKEFS_SECRET_KEY;
    this.objectsService.configuration.username = LAKEFS_ACCESS_KEY;
    this.objectsService.configuration.password = LAKEFS_SECRET_KEY;

    this.commitsService.configuration.username = LAKEFS_ACCESS_KEY;
    this.commitsService.configuration.password = LAKEFS_SECRET_KEY;

    this.s3Client = new S3Client({
      region: 'us-east-2',
      endpoint: 'http://localhost:4200/s3',
      credentials: {
        accessKeyId: LAKEFS_ACCESS_KEY,
        secretAccessKey: LAKEFS_SECRET_KEY
      },
      forcePathStyle: true,
      requestChecksumCalculation: "WHEN_REQUIRED"
    });
  }

  public createDataset(
    dataset: Dataset,
    initialVersionName: string,
    filesToBeUploaded: FileUploadItem[]
  ): Observable<DashboardDataset> {
    return new Observable<DashboardDataset>(subscriber => {
      const repositoryData: RepositoryCreation = {
        name: dataset.name,
        storage_namespace: `s3://${defaultEnvironment.s3BucketName}/${dataset.name}`,
        default_branch: 'main'
      }

      this.repositoriesService.createRepository(repositoryData, false).subscribe({
        next: (repository) => {
          const uploadPromises = filesToBeUploaded.map(file =>
            this.uploadFileS3(dataset.name, "main", file.name, file.file)
          );

          Promise.all(uploadPromises)
            .then(() => {
              let did = dataset.name ? dataset.name : "";
              this.createCommit(did, dataset.description).subscribe({
                next: (commit) => {
                  let createDataset: Dataset = {
                    did: repository.id,
                    ownerUid: 1,
                    name: repository.id,
                    isPublic: 1,
                    storagePath: undefined,
                    description: "",
                    creationTime: repository.creation_date,
                    versionHierarchy: []
                  }
                  let createDashboardDataset: DashboardDataset = {
                    isOwner: true,
                    ownerEmail: "zhey16@uci.edu",
                    dataset: createDataset,
                    accessPrivilege: "WRITE",
                    versions: [],
                    size: 1
                  }
                  subscriber.next(createDashboardDataset);
                  subscriber.complete();
                },
                error: (error) => {
                  return throwError(() => error);
                }
              })
            })
            .catch(error => {
              return throwError(() => error);
            });
        },
        error: (error) => {
          return throwError(() => error);
        }
      });
    });
  }
  
  private createCommit(
    did: string,
    description: string,
  ): Observable<Commit> {
    const commitParams = {
      message: description,
    }

    return this.commitsService.commit(commitParams, did, "main")
  }

  private async uploadFileS3(
    repo: string,
    branch: string,
    key: string, 
    file: File
  ): Promise<void> {
    if (file.size > CHUNK_SIZE) {
      // Use multipart upload for large files
      await this.uploadFileMultipartS3(repo, branch, key, file);
    } else {
      // Use simple put object for smaller files
      const commandParams = {
        Bucket: repo,
        Key: `${branch}/${key}`,
        Body: file,
        ContentType: file.type,
      };
  
      const command = new PutObjectCommand(commandParams);
  
      return this.s3Client.send(command)
        .then((data) => {
          console.log(`File ${key} uploaded successfully:`, data);
        })
        .catch((error) => {
          console.error(`Error uploading ${key} to LakeFS:`, error);
          throw error;
        });
    }
  }

  private async uploadFileMultipartS3(
    repo: string,
    branch: string,
    key: string,
    file: File
  ): Promise<void> {
    const objectKey = `${branch}/${key}`;
    const partCount = Math.ceil(file.size / CHUNK_SIZE);
    let uploadId: string | undefined;
  
    try {
      const createUploadResponse = await this.s3Client.send(
        new CreateMultipartUploadCommand({
          Bucket: repo,
          Key: objectKey,
          ContentType: file.type,
        })
      );
  
      uploadId = createUploadResponse.UploadId;
      if (!uploadId) throw new Error("Failed to initiate multipart upload");
  
      console.log(`Started multipart upload for ${key} with UploadId: ${uploadId}`);
  
      // Step 2: Upload Parts
      const uploadPromises = [];
      const uploadedParts: { PartNumber: number; ETag: string | undefined }[] = [];
  
      for (let i = 0; i < partCount; i++) {
        const start = i * CHUNK_SIZE;
        const end = Math.min(start + CHUNK_SIZE, file.size);
        const chunk = file.slice(start, end);
  
        uploadPromises.push(
          (async () => {
            const uploadPartResponse = await this.s3Client.send(
              new UploadPartCommand({
                Bucket: repo,
                Key: objectKey,
                UploadId: uploadId,
                PartNumber: i + 1,
                Body: chunk,
              })
            );
  
            console.log(`Uploaded part ${i + 1} of ${partCount}`);
  
            uploadedParts.push({
              PartNumber: i + 1,
              ETag: uploadPartResponse.ETag,
            });
          })()
        );
      }
  
      await Promise.all(uploadPromises);
  
      // Step 3: Complete Multipart Upload
      await this.s3Client.send(
        new CompleteMultipartUploadCommand({
          Bucket: repo,
          Key: objectKey,
          UploadId: uploadId,
          MultipartUpload: { Parts: uploadedParts.sort((a, b) => a.PartNumber - b.PartNumber) },
        })
      );
  
      console.log(`Multipart upload for ${key} completed successfully!`);
    } catch (error) {
      console.error(`Multipart upload failed for ${key}`, error);

      if (uploadId) {
        await this.s3Client.send(
          new AbortMultipartUploadCommand({
            Bucket: repo,
            Key: objectKey,
            UploadId: uploadId,
          })
        );
  
        console.error(`Upload aborted for ${key}`);
      }
    }
  }

  public getDataset(did: string, isLogin: boolean = true): Observable<DashboardDataset> {
    return new Observable<DashboardDataset>(subscriber => {
      this.repositoriesService.getRepository(did).subscribe({
        next: (repository) => {
          let createDataset: Dataset = {
            did: repository.id,
            ownerUid: 1,
            name: repository.id,
            isPublic: 1,
            storagePath: undefined,
            description: "",  
            creationTime: repository.creation_date,
            versionHierarchy: []
          }
          let createDashboardDataset: DashboardDataset = {
            isOwner: true,
            ownerEmail: "zhey16@uci.edu",
            dataset: createDataset,
            accessPrivilege: "WRITE",
            versions: [],
            size: 1
          }
          subscriber.next(createDashboardDataset);
          subscriber.complete();
        }
      })
    })
  }

  public retrieveDatasetVersionSingleFile(path: string, did: string, dvid: string): Observable<Blob> {
    return new Observable<Blob>(subscriber => {
      this.objectsService.getObject(did, dvid, path.substring(1)).subscribe({
        next: (data) => {
          subscriber.next(data);
          subscriber.complete();
        },
        error: (error) => {
          return throwError(() => error);
        }
      })
    })
  }

  public retrieveDatasetZip(options: { did: string; dvid?: string }): Observable<Blob> {
    return new Observable<Blob>();
  }

  public retrieveAccessibleDatasets(): Observable<DashboardDataset[]> {
    return new Observable<DashboardDataset[]>(subscriber => {
      console.log("retrieveAccessibleDatasets");
      this.repositoriesService.listRepositories().subscribe({
        next: (repositories) => {
          let createDashboardDatasets: DashboardDataset[] = repositories.results.map(repository => {
            let createDataset: Dataset = {
              did: repository.id,
              ownerUid: 1,
              name: repository.id,
              isPublic: 1,
              storagePath: undefined,
              description: "",
              creationTime: repository.creation_date,
              versionHierarchy: []
            }
            let createDashboardDataset: DashboardDataset = {
              isOwner: true,
              ownerEmail: "zhey16@uci.edu",
              dataset: createDataset,
              accessPrivilege: "WRITE",
              versions: [],
              size: 1
            }
            return createDashboardDataset;
          })
          subscriber.next(createDashboardDatasets);
          subscriber.complete();
        },
        error: (error) => {
          return throwError(() => error);
        }
      })
    })
  }

  public createDatasetVersion(
    did: string,
    newVersion: string,
    removedFilePaths: string[],
    filesToBeUploaded: FileUploadItem[]
  ): Observable<DatasetVersion> {
    return new Observable<DatasetVersion>(subscriber => {
      if (filesToBeUploaded.length === 0) {
        if (removedFilePaths.length > 0) {
          let deletePaths: PathList = {
            "paths": removedFilePaths.map(
              path => path.substring(1)
            )
          };
          this.objectsService.deleteObjects(deletePaths, did, "main").subscribe({
            next: (data) => {
              this.createCommit(did, newVersion).subscribe({
                next: (commit) => {
                  let createDatasetVersion: DatasetVersion = {
                    dvid: commit.id,
                    did: did,
                    creatorUid: 1,
                    name: newVersion,
                    versionHash: undefined,
                    creationTime: commit.creation_date,
                    fileNodes: []
                  }
                  subscriber.next(createDatasetVersion);
                  subscriber.complete();
                },
                error: (error) => {
                  return throwError(() => error);
                }
              })
            },
            error: (error) => {
              return throwError(() => error);
            }
          });
        } else {
          this.createCommit(did, newVersion).subscribe({
            next: (commit) => {
              let createDatasetVersion: DatasetVersion = {
                dvid: commit.id,
                did: did,
                creatorUid: 1,
                name: newVersion,
                versionHash: undefined,
                creationTime: commit.creation_date,
                fileNodes: []
              }
              subscriber.next(createDatasetVersion);
              subscriber.complete();
            },
            error: (error) => {
              return throwError(() => error);
            }
          })
        }
      } else {
        const uploadPromises = filesToBeUploaded.map(file =>
          this.uploadFileS3(did, "main", file.name, file.file)
        );
  
        Promise.all(uploadPromises)
          .then(() => {
            if (removedFilePaths.length > 0) {
              let deletePaths: PathList = {
                "paths": removedFilePaths.map(
                  path => path.substring(1)
                )
              };
    
              this.objectsService.deleteObjects(deletePaths, did, "main").subscribe({
                next: (data) => {
                  this.createCommit(did, newVersion).subscribe({
                    next: (commit) => {
                      let createDatasetVersion: DatasetVersion = {
                        dvid: commit.id,
                        did: did,
                        creatorUid: 1,
                        name: newVersion,
                        versionHash: undefined,
                        creationTime: commit.creation_date,
                        fileNodes: []
                      }
                      subscriber.next(createDatasetVersion);
                      subscriber.complete();
                    },
                    error: (error) => {
                      return throwError(() => error);
                    }
                  })
                },
                error: (error) => {
                  return throwError(() => error);
                }
              });
            } else {
              this.createCommit(did, newVersion).subscribe({
                next: (commit) => {
                  let createDatasetVersion: DatasetVersion = {
                    dvid: commit.id,
                    did: did,
                    creatorUid: 1,
                    name: newVersion,
                    versionHash: undefined,
                    creationTime: commit.creation_date,
                    fileNodes: []
                  }
                  subscriber.next(createDatasetVersion);
                  subscriber.complete();
                },
                error: (error) => {
                  return throwError(() => error);
                }
              })
            }
          })
          .catch((error) => {
            return throwError(() => error);
          });
      }
    })
  }

  /**
   * retrieve a list of versions of a dataset. The list is sorted so that the latest versions are at front.
   * @param did
   * @param isLogin
   */
  public retrieveDatasetVersionList(did: string, isLogin: boolean = true): Observable<DatasetVersion[]> {
    return new Observable<DatasetVersion[]>(subscriber => {
      this.refsService.logCommits(did, "main").subscribe({
        next: (commits) => {
          let createDatasetVersions: DatasetVersion[] = commits.results.map(commit => {
            let createDatasetVersion: DatasetVersion = {
              dvid: commit.id,
              did: did,
              creatorUid: 1,
              name: commit.message,
              versionHash: undefined,
              creationTime: commit.creation_date,
              fileNodes: []
            }
            return createDatasetVersion;
          });
          subscriber.next(createDatasetVersions);
          subscriber.complete();
        },
        error: (error) => {
          return throwError(() => error);
        }
      })
    })
  }

  /**
   * retrieve the latest version of a dataset.
   * @param did
   */
  public retrieveDatasetLatestVersion(did: string): Observable<DatasetVersion> {
    return new Observable<DatasetVersion>(subscriber => {
      this.retrieveDatasetVersionList(did).subscribe({
        next: (datasetVersions) => {
          this.retrieveDatasetVersionFileTree(did, datasetVersions[0].dvid ?? "").subscribe({
            next: (fileNodes) => {
              let latestVersion: DatasetVersion = {
                dvid: datasetVersions[0].dvid,
                did: did,
                creatorUid: 1,
                name: datasetVersions[0].name,
                versionHash: datasetVersions[0].versionHash,
                creationTime: datasetVersions[0].creationTime,
                fileNodes: fileNodes.fileNodes
              }
              subscriber.next(latestVersion);
              subscriber.complete();
            },
            error: (error) => {
              return throwError(() => error);
            }
          })
        },
        error: (error) => {
          return throwError(() => error);
        }
      })
    })
  }

  /**
   * retrieve a list of nodes that represent the files in the version
   * @param did
   * @param dvid
   * @param isLogin
   */
  public retrieveDatasetVersionFileTree(
    did: string,
    dvid: string,
    isLogin: boolean = true
  ): Observable<{ fileNodes: DatasetFileNode[]; size: number }> {
    return new Observable<{ fileNodes: DatasetFileNode[]; size: number }>(subscriber => {
      this.objectsService.listObjects(did, dvid).subscribe({
        next: (objects) => {
          let totalSize = 0;
          let fileNodes: DatasetFileNode[] = objects.results.map(object => {
            totalSize += object.size_bytes ?? 0;
            return {
              name: object.path,
              size: object.size_bytes ?? 0,
              type: "file",
              parentDir: ""
            }
          });
          subscriber.next({ fileNodes: fileNodes, size: totalSize });
          subscriber.complete();
        },
        error: (error) => {
          return throwError(() => error);
        }
      })
    })
  }
  
  public deleteDataset(repo: string,): Observable<Response> {
    return this.repositoriesService.deleteRepository(repo);
  }

  public deleteDatasets(dids: string[]): Observable<Response> {
    dids.forEach(did => {
      this.deleteDataset(did).subscribe({
        error: (error) => {
          return throwError(() => error);
        }
      })
    })

    return new Observable<Response>(subscriber => {
      subscriber.next({} as Response);
      subscriber.complete();
    })
  }

  public updateDatasetName(did: string, name: string): Observable<Response> {
    return new Observable<Response>(subscriber => {
      subscriber.next({} as Response);
      subscriber.complete();
    })
  }

  public updateDatasetDescription(did: string, description: string): Observable<Response> {
    return new Observable<Response>(subscriber => {
      subscriber.next({} as Response);
      subscriber.complete();
    })
  }

  public updateDatasetPublicity(did: string): Observable<Response> {
    return new Observable<Response>(subscriber => {
      subscriber.next({} as Response);
      subscriber.complete();
    })
  }

  public getDatasetOwners(did: string): Observable<number[]> {
    return new Observable<number[]>(subscriber => {
      subscriber.next([1]);
      subscriber.complete();
    })
  }
}