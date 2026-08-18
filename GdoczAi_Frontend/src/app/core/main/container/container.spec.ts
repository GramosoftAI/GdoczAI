import { ComponentFixture, TestBed } from '@angular/core/testing';

import { Container } from './container';

describe('Container', () => {
  let component: Container;
  let fixture: ComponentFixture<Container>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [Container]
    })
    .compileComponents();

    fixture = TestBed.createComponent(Container);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
