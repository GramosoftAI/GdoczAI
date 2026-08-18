import { ComponentFixture, TestBed } from '@angular/core/testing';

import { PromptTemplateForm } from './prompt-template-form';

describe('PromptTemplateForm', () => {
  let component: PromptTemplateForm;
  let fixture: ComponentFixture<PromptTemplateForm>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [PromptTemplateForm]
    })
    .compileComponents();

    fixture = TestBed.createComponent(PromptTemplateForm);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
