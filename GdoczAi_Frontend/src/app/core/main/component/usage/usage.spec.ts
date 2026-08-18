import { ComponentFixture, TestBed } from '@angular/core/testing';

import { Usage } from './usage';

describe('Usage', () => {
  let component: Usage;
  let fixture: ComponentFixture<Usage>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [Usage]
    })
    .compileComponents();

    fixture = TestBed.createComponent(Usage);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
