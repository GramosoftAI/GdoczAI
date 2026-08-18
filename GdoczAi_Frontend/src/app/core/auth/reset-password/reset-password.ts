import { CommonModule } from '@angular/common';
import { Component, EventEmitter, inject, OnDestroy, OnInit, Output } from '@angular/core';
import { FormBuilder, FormGroup, ReactiveFormsModule, Validators } from '@angular/forms';
import { ActivatedRoute, Router } from '@angular/router';
import { Subject } from 'rxjs';
import { takeUntil } from 'rxjs/operators';
import { ApiService } from '../../dependencies/services/api.service';
import { URLS } from '../../dependencies/config/api.config';

@Component({
  selector: 'app-reset-password',
  imports: [CommonModule, ReactiveFormsModule],
  templateUrl: './reset-password.html',
  styleUrl: './reset-password.scss',
})
export class ResetPassword implements OnInit, OnDestroy {
  private fb = inject(FormBuilder);
  private router = inject(Router);
  private route = inject(ActivatedRoute);
  private destroy$ = new Subject<void>();
  private apiService = inject(ApiService);

  @Output() formSubmitted = new EventEmitter<any>();
  @Output() viewChange = new EventEmitter<string>();

  rPasswordForm: FormGroup;
  isPassView: boolean = false;
  submitted = false;
  userData: any;
  token: any;

  constructor() {
    this.rPasswordForm = this.fb.group({
      new_password: ['', [Validators.required, Validators.pattern(/^(?=.*[A-Z])(?=.*[a-z])(?=.*\d)(?=.*[^\w\s]).{8,}$/)]],
      confirm_password: ['', [Validators.required]]
    }, { validators: this.passwordMatchValidator });
  }

  ngOnInit() {
    this.trimTocken();
  }

  trimTocken() {
    this.route.queryParams.pipe(takeUntil(this.destroy$)).subscribe(params => {
      this.token = params['token'];
    });
  }

  onSubmit() {
    this.submitted = true;

    if (this.rPasswordForm.invalid) {
      this.markAllAsTouched();
      return;
    }

    const formData = {
      new_password: this.rPasswordForm.value.new_password,
      confirm_password: this.rPasswordForm.value.confirm_password,
      token: this.token,
    };

    this.apiService.post(`${URLS.auth}/reset-password`, formData).subscribe({
      next: (res: any) => {
        if (res.success) {
          // alert('Password reset successfully!');
          this.router.navigate(['/auth/sign_in']);
        }
      },
      error: (error) => {
        console.error('Error resetting password:', error);
        alert('Failed to reset password. Please try again.');
      }
    });
  }

  private markAllAsTouched(): void {
    Object.keys(this.rPasswordForm.controls).forEach(key => {
      this.rPasswordForm.get(key)?.markAsTouched();
    });
  }

  togglePasswordView() {
    this.isPassView = !this.isPassView;
  }

  // Custom validator for password matching
  passwordMatchValidator(form: FormGroup) {
    const new_password = form.get('new_password')?.value;
    const confirm_password = form.get('confirm_password')?.value;

    if (new_password !== confirm_password) {
      form.get('confirm_password')?.setErrors({ passwordMismatch: true });
      return { passwordMismatch: true };
    } else {
      form.get('confirm_password')?.setErrors(null);
      return null;
    }
  }

  ngOnDestroy() {
    this.destroy$.next();
    this.destroy$.complete();
  }
}
