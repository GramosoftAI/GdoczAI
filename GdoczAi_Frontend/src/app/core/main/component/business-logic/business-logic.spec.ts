import { ComponentFixture, TestBed } from '@angular/core/testing';

import { BusinessLogic } from './business-logic';

describe('BusinessLogic', () => {
  let component: BusinessLogic;
  let fixture: ComponentFixture<BusinessLogic>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [BusinessLogic]
    })
    .compileComponents();

    fixture = TestBed.createComponent(BusinessLogic);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
