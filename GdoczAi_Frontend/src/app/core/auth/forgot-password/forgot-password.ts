import { Component, ChangeDetectorRef, inject, EventEmitter, Output } from '@angular/core';
import { FormBuilder, FormGroup, Validators, ReactiveFormsModule } from '@angular/forms';
import { CommonModule } from '@angular/common';
import { Router, RouterModule } from '@angular/router';
import { Subject } from 'rxjs';
// import { ToastrService } from 'ngx-toastr';
// import { AuthService } from '../../dependencies/services/auth.service';

@Component({
  selector: 'app-forgot-password',
  standalone: true,
  imports: [CommonModule, ReactiveFormsModule, RouterModule],
  templateUrl: './forgot-password.html',
  styleUrl: './forgot-password.scss'
})
export class ForgotPassword {
  private fb = inject(FormBuilder);
  private router = inject(Router);
  private destroy$ = new Subject<void>();

  @Output() formSubmitted = new EventEmitter<any>();
  @Output() viewChange = new EventEmitter<string>();

  fpasswordForm: FormGroup;
  submitted = false;

  constructor() {
    this.fpasswordForm = this.fb.group({
      email: ['', [Validators.required, Validators.pattern(/^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$/)]]
    });
  }

  onSubmit() {
    this.submitted = true;

    if (this.fpasswordForm.invalid) {
      this.markAllAsTouched();
      return;
    }

    // Emit the form data to parent component
    this.formSubmitted.emit(this.fpasswordForm.value);
  }

  private markAllAsTouched(): void {
    Object.keys(this.fpasswordForm.controls).forEach(key => {
      this.fpasswordForm.get(key)?.markAsTouched();
    });
  }

  ngOnDestroy() {
    this.destroy$.next();
    this.destroy$.complete();
  }
}
