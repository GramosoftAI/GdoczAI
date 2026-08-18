import { ComponentFixture, TestBed } from '@angular/core/testing';

import { PromtTemplate } from './promt-template';

describe('PromtTemplate', () => {
  let component: PromtTemplate;
  let fixture: ComponentFixture<PromtTemplate>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [PromtTemplate]
    })
    .compileComponents();

    fixture = TestBed.createComponent(PromtTemplate);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
