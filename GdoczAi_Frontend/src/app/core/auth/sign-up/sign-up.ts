import { CommonModule } from '@angular/common';
import { Component, Output, EventEmitter, inject, OnDestroy } from '@angular/core';
import { FormBuilder, FormGroup, ReactiveFormsModule, Validators } from '@angular/forms';
import { Router } from '@angular/router';
import { Subject } from 'rxjs';

@Component({
  selector: 'app-sign-up',
  standalone: true,
  imports: [CommonModule, ReactiveFormsModule],
  templateUrl: './sign-up.html',
  styleUrl: './sign-up.scss',
})
export class SignUp implements OnDestroy {
  private fb = inject(FormBuilder);
  private destroy$ = new Subject<void>();

  @Output() otpRequested = new EventEmitter<any>();
  @Output() formSubmitted = new EventEmitter<any>();
  @Output() viewChange = new EventEmitter<string>();

  signupForm: FormGroup;
  isPassView: boolean = false;
  submitted = false;

  constructor() {
    this.signupForm = this.fb.group({
      name: ['', [Validators.required, Validators.minLength(3)]],
      email: ['', [Validators.required, Validators.pattern(/^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$/)]],
      password: ['', [Validators.required, Validators.pattern(/^(?=.*[A-Z])(?=.*[a-z])(?=.*\d)(?=.*[^\w\s]).{8,}$/)]]
    });
  }

   onSubmit() {
    this.submitted = true;

    if (this.signupForm.invalid) {
      this.markAllAsTouched();
      return;
    }

    // Emit otpRequested instead of formSubmitted
    this.otpRequested.emit(this.signupForm.value);
  }

  private markAllAsTouched(): void {
    Object.keys(this.signupForm.controls).forEach(key => {
      this.signupForm.get(key)?.markAsTouched();
    });
  }

  togglePasswordView() {
    this.isPassView = !this.isPassView;
  }

  navigateToSignIn() {
    this.viewChange.emit('sign_in');
  }

  ngOnDestroy() {
    this.destroy$.next();
    this.destroy$.complete();
  }
}
