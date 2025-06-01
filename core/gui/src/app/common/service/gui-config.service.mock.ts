import { Injectable } from "@angular/core";
import { Observable, of } from "rxjs";
import { GuiConfig } from "../type/gui-config";

/**
 * Mock GuiConfigService for testing purposes.
 * Provides default configuration values without requiring HTTP calls.
 */
@Injectable()
export class MockGuiConfigService {
  private _config: GuiConfig = {
    apiUrl: "http://localhost:8080",
    exportExecutionResultEnabled: false,
    autoAttributeCorrectionEnabled: false,
    userSystemEnabled: true,
    selectingFilesFromDatasetsEnabled: false,
    localLogin: true,
    googleLogin: true,
    inviteOnly: false,
    userPresetEnabled: true,
    workflowExecutionsTrackingEnabled: false,
    linkBreakpointEnabled: false,
    asyncRenderingEnabled: false,
    timetravelEnabled: false,
    productionSharedEditingServer: false,
    singleFileUploadMaximumSizeMB: 100,
    maxNumberOfConcurrentUploadingFileChunks: 5,
    multipartUploadChunkSizeByte: 1048576, // 1MB
    defaultDataTransferBatchSize: 100,
    workflowEmailNotificationEnabled: false,
    hubEnabled: false,
    forumEnabled: false,
    projectEnabled: true,
    operatorConsoleMessageBufferSize: 1000,
    defaultLocalUser: { username: "", password: "" },
  };

  get env(): GuiConfig {
    return this._config;
  }

  load(): Observable<void> {
    // Mock implementation - immediately returns success
    return of(undefined);
  }

  // Allow tests to override specific config values
  setConfig(config: Partial<GuiConfig>): void {
    this._config = { ...this._config, ...config };
  }
}
