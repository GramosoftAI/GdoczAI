import { ComponentFixture, TestBed } from '@angular/core/testing';

import { TermsUser } from './terms-user';

describe('TermsUser', () => {
  let component: TermsUser;
  let fixture: ComponentFixture<TermsUser>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [TermsUser]
    })
    .compileComponents();

    fixture = TestBed.createComponent(TermsUser);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
