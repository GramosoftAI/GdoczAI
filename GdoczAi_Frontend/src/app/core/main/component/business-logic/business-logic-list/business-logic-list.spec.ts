import { ComponentFixture, TestBed } from '@angular/core/testing';

import { BusinessLogicList } from './business-logic-list';

describe('BusinessLogicList', () => {
  let component: BusinessLogicList;
  let fixture: ComponentFixture<BusinessLogicList>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [BusinessLogicList]
    })
    .compileComponents();

    fixture = TestBed.createComponent(BusinessLogicList);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
