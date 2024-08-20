import { ComponentFixture, TestBed } from '@angular/core/testing';

import { ClusterManagementModalComponent } from './cluster-management-modal.component';

describe('ClusterManagementModalComponent', () => {
  let component: ClusterManagementModalComponent;
  let fixture: ComponentFixture<ClusterManagementModalComponent>;

  beforeEach(() => {
    TestBed.configureTestingModule({
      declarations: [ClusterManagementModalComponent]
    });
    fixture = TestBed.createComponent(ClusterManagementModalComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
