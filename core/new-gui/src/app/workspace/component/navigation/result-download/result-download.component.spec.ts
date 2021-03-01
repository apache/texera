import { ComponentFixture, TestBed } from '@angular/core/testing';

import { ResultDownloadComponent } from './result-download.component';
import { NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';

describe('ResultDownloadComponent', () => {
  let component: ResultDownloadComponent;
  let fixture: ComponentFixture<ResultDownloadComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ ResultDownloadComponent ],
      providers: [NgbActiveModal]
    })
    .compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(ResultDownloadComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
