import { ComponentFixture, TestBed } from '@angular/core/testing';

import { PromptTemplateList } from './prompt-template-list';

describe('PromptTemplateList', () => {
  let component: PromptTemplateList;
  let fixture: ComponentFixture<PromptTemplateList>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [PromptTemplateList]
    })
    .compileComponents();

    fixture = TestBed.createComponent(PromptTemplateList);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
