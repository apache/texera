import { ComponentFixture, TestBed } from "@angular/core/testing";
import { OperatorMetadataService } from "src/app/workspace/service/operator-metadata/operator-metadata.service";
import { StubOperatorMetadataService } from "src/app/workspace/service/operator-metadata/stub-operator-metadata.service";

import { ContextMenuComponent } from "./context-menu.component";
import { HttpClientModule } from "@angular/common/http";

describe("ContextMenuComponent", () => {
  let component: ContextMenuComponent;
  let fixture: ComponentFixture<ContextMenuComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ContextMenuComponent],
      providers: [{ provide: OperatorMetadataService, useClass: StubOperatorMetadataService }],
      imports: [HttpClientModule],
    }).compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(ContextMenuComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it("should create", () => {
    expect(component).toBeTruthy();
  });
});
