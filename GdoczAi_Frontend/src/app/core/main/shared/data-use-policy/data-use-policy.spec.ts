import { ComponentFixture, TestBed } from '@angular/core/testing';

import { DataUsePolicy } from './data-use-policy';

describe('DataUsePolicy', () => {
  let component: DataUsePolicy;
  let fixture: ComponentFixture<DataUsePolicy>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [DataUsePolicy]
    })
    .compileComponents();

    fixture = TestBed.createComponent(DataUsePolicy);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
