import { CommonModule } from '@angular/common';
import { Component, inject, OnDestroy, OnInit } from '@angular/core';
import { AbstractControl, FormControl, FormGroup, FormsModule, ReactiveFormsModule, Validators } from '@angular/forms';
import { Subject, takeUntil } from 'rxjs';
import { ApiService } from '../../../dependencies/services/api.service';
import { URLS } from '../../../dependencies/config/api.config';

@Component({
  selector: 'app-webhook',
  imports: [CommonModule, FormsModule, ReactiveFormsModule],
  templateUrl: './webhook.html',
  styleUrl: './webhook.scss',
})
export class Webhook implements OnInit, OnDestroy {
  readonly apiService = inject(ApiService);
  webhookForm: FormGroup;
  alertMailForm: FormGroup;
  unSubscribe$ = new Subject<void>();
  hasWebhookData: boolean = false;
  hasAlertMailData: boolean = false;

  // Chip management
  emailChips: string[] = [];
  isEditingChip: boolean = false;
  editingChipIndex: number = -1;

  constructor() {
    this.webhookForm = new FormGroup({
      webhook_url: new FormControl('', [Validators.required]),
      webhook_token: new FormControl('', [Validators.required]),
      webhook_agent_name: new FormControl('', [Validators.required]),
      is_active: new FormControl(true),
    });

    this.alertMailForm = new FormGroup({
      cc_mail: new FormControl('', [this.singleEmailValidator]),
    });
  }

  // Validator for single email input
  singleEmailValidator(control: AbstractControl): { [key: string]: boolean } | null {
    if (!control.value || control.value.trim() === '') {
      return null;
    }

    const email = control.value.trim();
    const emailRegex = /^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$/;

    if (!emailRegex.test(email)) {
      return { 'invalidEmail': true };
    }

    return null;
  }

  ngOnInit(): void {
    this.getWebhookConfig();
    this.getAlertMail();
  }

  ngOnDestroy(): void {
    this.unSubscribe$.next();
    this.unSubscribe$.complete();
  }

  // Chip management methods
  addChip(email: string): void {
    const trimmedEmail = email.trim();
    if (!trimmedEmail || this.emailChips.includes(trimmedEmail)) {
      return;
    }

    // Clear the input field after adding
    this.emailChips.push(trimmedEmail);
    this.alertMailForm.get('cc_mail')?.reset('', { emitEvent: false });
    this.alertMailForm.get('cc_mail')?.markAsPristine();
    this.alertMailForm.get('cc_mail')?.markAsUntouched();
    this.isEditingChip = false;
    this.editingChipIndex = -1;
  }

  removeChip(index: number): void {
    this.emailChips.splice(index, 1);
    this.isEditingChip = false;
    this.editingChipIndex = -1;
    // Clear input if we were editing
    this.alertMailForm.get('cc_mail')?.reset('', { emitEvent: false });
  }

  editChip(index: number): void {
    if (index >= 0 && index < this.emailChips.length) {
      this.isEditingChip = true;
      this.editingChipIndex = index;
      this.alertMailForm.get('cc_mail')?.setValue(this.emailChips[index], { emitEvent: false });
      this.alertMailForm.get('cc_mail')?.markAsDirty();
      this.alertMailForm.get('cc_mail')?.markAsTouched();
    }
  }

  updateChip(): void {
    const email = this.alertMailForm.get('cc_mail')?.value?.trim();
    if (email && this.editingChipIndex >= 0) {
      this.emailChips[this.editingChipIndex] = email;
      // Clear input field after updating
      this.alertMailForm.get('cc_mail')?.reset('', { emitEvent: false });
      this.alertMailForm.get('cc_mail')?.markAsPristine();
      this.alertMailForm.get('cc_mail')?.markAsUntouched();
      this.isEditingChip = false;
      this.editingChipIndex = -1;
    }
  }

  addChipFromInput(event?: Event): void {
    if (event) {
      event.preventDefault();
      (event.target as HTMLInputElement).blur();
    }

    const email = this.alertMailForm.get('cc_mail')?.value?.trim();
    if (!email) {
      return;
    }

    // If we're editing a chip, update it
    if (this.isEditingChip && this.editingChipIndex >= 0) {
      this.updateChip();
    }
    // Otherwise add a new chip
    else {
      // Validate email before adding
      if (this.singleEmailValidator(new FormControl(email)) === null) {
        this.addChip(email);
      }
    }
  }

  onInputBlur(): void {
    setTimeout(() => {
      this.addChipFromInput();
    }, 150); // Small delay to allow click events on chips to fire first
  }

  getWebhookConfig() {
    this.apiService.get(`${URLS.webhook}`).pipe(takeUntil(this.unSubscribe$)).subscribe((res: any) => {
      if (res && res.success) {
        if (res.webhook) {
          this.hasWebhookData = true;
          this.webhookForm.patchValue({
            webhook_url: res.webhook.webhook_url,
            webhook_token: res.webhook.webhook_token,
            webhook_agent_name: res.webhook.webhook_agent_name,
            is_active: res.webhook.is_active,
          });
        } else {
          this.hasWebhookData = false;
          this.webhookForm.reset({ is_active: true });
        }
      }
    });
  }

  getAlertMail() {
    this.apiService.get(`${URLS.alertMail}`).pipe(takeUntil(this.unSubscribe$)).subscribe((res: any) => {
      console.log('Alert Mail Response:', res);

      if (res && res.success) {
        let emailString = '';

        if (res.alert_mail && res.alert_mail.cc_mail) {
          emailString = res.alert_mail.cc_mail;
        }
        else if (res.cc_mail) {
          emailString = res.cc_mail;
        }
        else if (res.webhook && res.webhook.cc_mail) {
          emailString = res.webhook.cc_mail;
        }

        if (emailString) {
          this.hasAlertMailData = true;
          // Parse comma-separated emails into chips
          this.emailChips = emailString.split(',')
            .map(email => email.trim())
            .filter(email => email.length > 0);
          // Clear the input field
          this.alertMailForm.get('cc_mail')?.reset('', { emitEvent: false });
        } else {
          this.hasAlertMailData = false;
          this.emailChips = [];
          this.alertMailForm.reset();
        }
      } else {
        this.hasAlertMailData = false;
        this.emailChips = [];
        this.alertMailForm.reset();
      }
    }, (error) => {
      console.error('Error fetching alert mail:', error);
      this.hasAlertMailData = false;
      this.emailChips = [];
      this.alertMailForm.reset();
    });
  }

  isFieldInvalid(fieldName: string): boolean {
    const field = this.webhookForm.get(fieldName);
    return !!(field && field.invalid && (field.dirty || field.touched));
  }

  isAlertMailInvalid(): boolean {
    const field = this.alertMailForm.get('cc_mail');
    // Only show error if input has value and is invalid
    return !!(field && field.value && field.invalid && (field.dirty || field.touched));
  }

  onSubmit(): void {
    if (this.webhookForm.invalid) {
      return;
    }

    if (this.hasWebhookData) {
      this.updateWebhook();
    } else {
      this.createWebhook();
    }
  }

  createWebhook(): void {
    this.apiService.post(`${URLS.webhook}`, this.webhookForm.value).pipe(takeUntil(this.unSubscribe$)).subscribe((res: any) => {
      if (res && res.success) {
        this.getWebhookConfig();
      }
    });
  }

  updateWebhook(): void {
    this.apiService.put(`${URLS.webhook}`, this.webhookForm.value).pipe(takeUntil(this.unSubscribe$)).subscribe((res: any) => {
      if (res && res.success) {
        this.getWebhookConfig();
      }
    });
  }

  onAlertMailSubmit(): void {
    if (this.emailChips.length === 0) {
      return;
    }

    // Clear the input field before submitting
    this.alertMailForm.get('cc_mail')?.reset('', { emitEvent: false });

    // Prepare payload with comma-separated emails
    const payload = {
      cc_mail: this.emailChips.join(', ')
    };

    if (this.hasAlertMailData) {
      this.updateAlertMail(payload);
    } else {
      this.addAlertMail(payload);
    }
  }

  addAlertMail(payload: any): void {
    this.apiService.post(`${URLS.alertMail}`, payload)
      .pipe(takeUntil(this.unSubscribe$))
      .subscribe((res: any) => {
        if (res) {
          console.log('Alert Mail Added Successfully:', res);
          this.getAlertMail();
        }
      });
  }

  updateAlertMail(payload: any): void {
    this.apiService.put(`${URLS.alertMail}`, payload)
      .pipe(takeUntil(this.unSubscribe$))
      .subscribe((res: any) => {
        if (res) {
          console.log('Alert Mail Updated Successfully:', res);
          this.getAlertMail();
        }
      });
  }

  deleteAlertMail(): void {
    this.apiService.delete(`${URLS.alertMail}`)
      .pipe(takeUntil(this.unSubscribe$))
      .subscribe((res: any) => {
        if (res && res.success) {
          console.log('Alert Mail Deleted Successfully:', res);
          this.getAlertMail();
        }
      });
  }
}
