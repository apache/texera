import { getTestBed } from "@angular/core/testing";
import { BrowserDynamicTestingModule, platformBrowserDynamicTesting } from "@angular/platform-browser-dynamic/testing";
import { NzMessageModule } from "ng-zorro-antd/message";

getTestBed().initTestEnvironment([BrowserDynamicTestingModule, NzMessageModule], platformBrowserDynamicTesting(), {
  teardown: { destroyAfterEach: false },
});
