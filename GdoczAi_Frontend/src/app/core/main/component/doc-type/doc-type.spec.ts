import { ComponentFixture, TestBed } from '@angular/core/testing';

import { DocType } from './doc-type';

describe('DocType', () => {
  let component: DocType;
  let fixture: ComponentFixture<DocType>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [DocType]
    })
    .compileComponents();

    fixture = TestBed.createComponent(DocType);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
