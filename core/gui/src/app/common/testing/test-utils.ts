import { HttpClientTestingModule } from "@angular/common/http/testing";
import { Provider } from "@angular/core";
import { GuiConfigService } from "../service/gui-config.service";
import { MockGuiConfigService } from "../service/gui-config.service.mock";

/**
 * Common test providers that can be used across all spec files
 */
export const commonTestProviders: Provider[] = [{ provide: GuiConfigService, useClass: MockGuiConfigService }];

/**
 * Common test module imports
 */
export const commonTestImports = [HttpClientTestingModule];
