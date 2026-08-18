import { ComponentFixture, TestBed } from '@angular/core/testing';

import { OtpVerify } from './otp-verify';

describe('OtpVerify', () => {
  let component: OtpVerify;
  let fixture: ComponentFixture<OtpVerify>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [OtpVerify]
    })
    .compileComponents();

    fixture = TestBed.createComponent(OtpVerify);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
