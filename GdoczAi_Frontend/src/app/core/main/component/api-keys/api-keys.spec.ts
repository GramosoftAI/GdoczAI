import { ComponentFixture, TestBed } from '@angular/core/testing';

import { ApiKeys } from './api-keys';

describe('ApiKeys', () => {
  let component: ApiKeys;
  let fixture: ComponentFixture<ApiKeys>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [ApiKeys]
    })
    .compileComponents();

    fixture = TestBed.createComponent(ApiKeys);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
