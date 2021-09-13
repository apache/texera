import { ComponentFixture, TestBed, waitForAsync } from "@angular/core/testing";

import { NotificationComponent } from "./notification.component";
import { NzMessageModule } from "ng-zorro-antd/message";

describe("NotificationComponent", () => {
  let component: NotificationComponent;
  let fixture: ComponentFixture<NotificationComponent>;

  beforeEach(
    waitForAsync(() => {
      TestBed.configureTestingModule({
        declarations: [NotificationComponent],
        imports: [NzMessageModule],
      }).compileComponents();
    })
  );

  beforeEach(() => {
    fixture = TestBed.createComponent(NotificationComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it("should create", () => {
    expect(component).toBeTruthy();
  });
});
