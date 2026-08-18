import { ComponentFixture, TestBed } from '@angular/core/testing';

import { BusinessLogicForm } from './business-logic-form';

describe('BusinessLogicForm', () => {
  let component: BusinessLogicForm;
  let fixture: ComponentFixture<BusinessLogicForm>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [BusinessLogicForm]
    })
    .compileComponents();

    fixture = TestBed.createComponent(BusinessLogicForm);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
