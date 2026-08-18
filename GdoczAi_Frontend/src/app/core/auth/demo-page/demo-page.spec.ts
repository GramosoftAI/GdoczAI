import { ComponentFixture, TestBed } from '@angular/core/testing';

import { DemoPage } from './demo-page';

describe('DemoPage', () => {
  let component: DemoPage;
  let fixture: ComponentFixture<DemoPage>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [DemoPage]
    })
    .compileComponents();

    fixture = TestBed.createComponent(DemoPage);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
