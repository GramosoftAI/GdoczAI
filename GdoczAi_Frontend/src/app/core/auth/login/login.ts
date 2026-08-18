import { Component, OnDestroy, inject } from '@angular/core';
import { FormBuilder, FormGroup, ReactiveFormsModule, Validators } from '@angular/forms';
import { Subject } from 'rxjs';
import { CommonModule } from '@angular/common';
import { Router, RouterModule } from '@angular/router';
import { SignUp } from '../sign-up/sign-up';
import { ForgotPassword } from "../forgot-password/forgot-password";
import { ResetPassword } from "../reset-password/reset-password";
import { URLS } from '../../dependencies/config/api.config';
import { ApiService } from '../../dependencies/services/api.service';
import { AuthService } from '../../dependencies/services/auth.service';
import { OtpVerify } from '../../dependencies/components/otp-verify/otp-verify';
// import { AuthService } from '../../dependencies/services/auth.service';

@Component({
  selector: 'app-login',
  standalone: true,
  imports: [CommonModule, ReactiveFormsModule, RouterModule, SignUp, ForgotPassword, OtpVerify],
  templateUrl: './login.html',
  styleUrl: './login.scss'
})
export class Login implements OnDestroy {
  private fb = inject(FormBuilder);
  public router = inject(Router);
  private destroy$ = new Subject<void>();
  readonly apiService = inject(ApiService);
  readonly authService = inject(AuthService);

  loginForm: FormGroup;

  isPassView: boolean = false;
  submitted = false;
  enableSignUp: string = 'sign_in';

  // OTP MODAL
  showOtpModal = false;
  pendingSignupData: any = null;
  isOtpVerifying = false;

  constructor() {
    this.loginForm = this.fb.group({
      email: ['', [Validators.required, Validators.pattern(/^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$/)]],
      password: ['', [Validators.required, Validators.pattern(/^(?=.*[A-Z])(?=.*[a-z])(?=.*\d)(?=.*[^\w\s]).{8,}$/)]]
    });
  }

  async loginSubmit() {
    this.submitted = true;

    let currentForm: FormGroup;
    switch (this.enableSignUp) {
      case 'sign_in':
        currentForm = this.loginForm;
        break;
      default:
        currentForm = this.loginForm;
    }

    this.apiService.post(`${URLS.auth}/signin`, this.loginForm.value).subscribe((res: any) => {
      if (res.success) {
        this.authService.login(res);
        this.router.navigate(['/main/playground']);
      }
    });

    if (currentForm.invalid) {
      this.markAllAsTouched(currentForm);
      return;
    }
  }

   onSignUpSubmit(formData: any) {
    this.apiService.post(`${URLS.auth}/signup/send-otp`, formData).subscribe({
      next: (res: any) => {
        if (res.success) {
          this.pendingSignupData = formData;
          this.showOtpModal = true;
        }
      },
      error: (error: any) => {
        console.error('Error sending OTP:', error);
      }
    });
  }

  onFpasswordSubmit(formData: any) {
    this.apiService.post(`${URLS.auth}/forgot-password`, formData).subscribe((res: any) => {
      if (res.success) {
        this.changePage('reset_password');
      }
    });
  }

  onViewChange(view: string) {
    this.changePage(view);
  }

  private markAllAsTouched(form: FormGroup): void {
    Object.keys(form.controls).forEach(key => {
      form.get(key)?.markAsTouched();
    });
  }

  changePage(path: string) {
    this.submitted = false;

    if (path === 'sign_up') {
      this.enableSignUp = 'sign_up';
    } else if (path === 'sign_in') {
      this.enableSignUp = 'sign_in';
    } else if (path === 'forgot_password') {
      this.enableSignUp = 'forgot_password';
    } else if (path === 'reset_password') {
      this.enableSignUp = 'reset_password';
    }
  }

  onVerifyOtp(otp: string) {
    this.isOtpVerifying = true;

    const formData = new FormData();
    formData.append('email', this.pendingSignupData.email);
    formData.append('otp', otp);

    this.apiService.post(`${URLS.auth}/signup/verify-otp`, formData).subscribe({
      next: (res: any) => {
        this.isOtpVerifying = false;

        if (res.success) {
          // OTP verified successfully, navigate to sign in
          this.showOtpModal = false;
          this.pendingSignupData = null;
          this.changePage('sign_in');
        } else {
          console.error('OTP verification failed:', res.message);
        }
      },
      error: (error: any) => {
        this.isOtpVerifying = false;
        console.error('Error verifying OTP:', error);
      }
    });
  }

  onResendOtp() {
    if (!this.pendingSignupData) return;

    this.apiService.post(`${URLS.auth}/signup/send-otp`, this.pendingSignupData).subscribe({
      next: (res: any) => {
        if (res.success) {
          console.log('OTP resent successfully!');
        }
      },
      error: (error: any) => {
        console.error('Error resending OTP:', error);
      }
    });
  }

  onCloseOtpModal() {
    this.showOtpModal = false;
    this.pendingSignupData = null;
    this.isOtpVerifying = false;
  }

  onOtpRequested(formData: any) {
    this.onSignUpSubmit(formData);
  }

  togglePasswordView() {
    this.isPassView = !this.isPassView;
  }

  ngOnDestroy() {
    this.destroy$.next();
    this.destroy$.complete();
  }
}
