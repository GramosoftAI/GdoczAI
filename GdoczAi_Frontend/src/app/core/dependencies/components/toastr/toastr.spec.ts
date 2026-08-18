import { ComponentFixture, TestBed } from '@angular/core/testing';

import { Toastr } from './toastr';

describe('Toastr', () => {
  let component: Toastr;
  let fixture: ComponentFixture<Toastr>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [Toastr]
    })
    .compileComponents();

    fixture = TestBed.createComponent(Toastr);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
