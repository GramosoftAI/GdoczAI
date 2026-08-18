import { CommonModule } from '@angular/common';
import { Component, EventEmitter, Output, inject, ViewChild, OnDestroy, ChangeDetectorRef } from '@angular/core';
import { FormBuilder, FormGroup, ReactiveFormsModule, Validators } from '@angular/forms';
import { NgOtpInputModule } from 'ng-otp-input';
import { Subscription, timer } from 'rxjs';

@Component({
  selector: 'app-otp-verify',
  standalone: true,
  imports: [CommonModule, ReactiveFormsModule, NgOtpInputModule],
  templateUrl: './otp-verify.html',
  styleUrl: './otp-verify.scss',
})
export class OtpVerify implements OnDestroy {
  private fb = inject(FormBuilder);
  private cdRef = inject(ChangeDetectorRef);

  @Output() verifyOtp = new EventEmitter<string>();
  @Output() closeModal = new EventEmitter<void>();
  @Output() resendOtp = new EventEmitter<void>();

  @ViewChild('ngOtpInput') ngOtpInput: any;

  otpValue: string = '';
  submitted = false;
  isLoading = false;
  isResending = false;
  errorMessage = '';
  counter: number = 30;
  countDown!: Subscription;

  // OTP Configuration remains the same...
  otpConfig = {
    length: 5,
    inputClass: 'otp-input',
    containerClass: 'otp-container',
    inputStyles: {
      'width': '50px',
      'height': '50px',
      'border': '2px solid #d1d5db',
      'border-radius': '8px',
      'font-size': '18px',
      'font-weight': '600',
      'color': '#1f2937',
      'margin': '0 8px',
      'text-align': 'center',
      'transition': 'all 0.2s',
      'background': '#ffffff'
    },
    allowNumbersOnly: true,
    autoFocus: true,
    inputStylesFilled: {
      'border': '2px solid #3b82f6',
      'background-color': '#f8fafc'
    },
    inputStylesFocus: {
      'border': '2px solid #3b82f6',
      'box-shadow': '0 0 0 3px rgba(59, 130, 246, 0.1)',
      'outline': 'none'
    }
  };

  ngOnInit() {
    this.startCountdown();
  }

  onOtpChange(otp: string) {
    this.otpValue = otp;
    this.errorMessage = '';

    if (otp.length === 5) {
      setTimeout(() => {
        this.onSubmit();
      }, 300);
    }
  }

  onSubmit() {
    this.submitted = true;

    if (this.otpValue.length !== 5) {
      this.errorMessage = 'Please enter a complete 5-digit OTP code.';
      return;
    }

    this.isLoading = true;
    this.errorMessage = '';
    this.verifyOtp.emit(this.otpValue);
  }

  onResendOtp() {
    if (this.counter > 0) return;

    this.isResending = true;
    this.errorMessage = '';

    this.resendOtp.emit();
    this.startCountdown();

    setTimeout(() => {
      this.isResending = false;
    }, 2000);
  }

  onClose() {
    this.closeModal.emit();
  }

  private startCountdown() {
    this.countDown?.unsubscribe();
    this.counter = 30;
    this.countDown = timer(0, 1000).subscribe(() => {
      --this.counter;
      this.cdRef.detectChanges();
      if (this.counter <= 0) {
        this.countDown.unsubscribe();
      }
    });
  }

  // Method to reset form state
  resetForm() {
    this.isLoading = false;
    this.submitted = false;
    this.errorMessage = '';
    this.otpValue = '';

    if (this.ngOtpInput) {
      this.ngOtpInput.setValue('');
    }
  }

  // Method to set error message
  setError(message: string) {
    this.errorMessage = message;
    this.isLoading = false;
  }

  ngOnDestroy() {
    if (this.countDown) {
      this.countDown.unsubscribe();
    }
  }
}
