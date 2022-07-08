import { ComponentFixture, TestBed } from '@angular/core/testing';

import { DeletePromptComponent } from './delete-prompt.component';

describe('DeletePromptComponent', () => {
  let component: DeletePromptComponent;
  let fixture: ComponentFixture<DeletePromptComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ DeletePromptComponent ]
    })
    .compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(DeletePromptComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
