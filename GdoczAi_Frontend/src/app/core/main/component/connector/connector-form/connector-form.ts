import { CommonModule } from '@angular/common';
import { Component, inject, OnDestroy, OnInit } from '@angular/core';
import { AbstractControl, FormControl, FormGroup, FormsModule, ReactiveFormsModule, Validators } from '@angular/forms';
import { ActivatedRoute, Router } from '@angular/router';
import { Subject, takeUntil } from 'rxjs';
import { ApiService } from '../../../../dependencies/services/api.service';
import { URLS } from '../../../../dependencies/config/api.config';

type ConnectorTab = 'SFTP' | 'SMTP';
type EmailMethod = 'gmail' | 'outlook' | 'yahoo' | 'custom';

@Component({
  selector: 'app-connector-form',
  imports: [CommonModule, FormsModule, ReactiveFormsModule],
  templateUrl: './connector-form.html',
  styleUrl: './connector-form.scss',
})
export class ConnectorForm implements OnInit, OnDestroy {
  readonly apiService = inject(ApiService);
  readonly router = inject(Router);
  readonly route = inject(ActivatedRoute);

  activeTab: ConnectorTab = 'SFTP';
  emailMethod: EmailMethod = 'gmail';
  isEditMode = false;
  hasSftpData = false;
  hasSmtpData = false;
  showPassword = false;
  showSmtpPassword = false;
  isSubmitting = false;
  selectedPemFile: File | null = null;

  sftpForm: FormGroup;
  smtpForm: FormGroup;

  private unSubscribe$ = new Subject<void>();

  readonly emailMethodOptions = [
    { value: 'gmail', label: 'Gmail' },
    { value: 'outlook', label: 'Outlook / Office 365' },
    { value: 'yahoo', label: 'Yahoo Mail' },
    { value: 'custom', label: 'Custom SMTP' },
  ];

  readonly imapDefaults: Record<EmailMethod, { server: string; port: number }> = {
    gmail: { server: 'imap.gmail.com', port: 993 },
    outlook: { server: 'outlook.office365.com', port: 993 },
    yahoo: { server: 'imap.mail.yahoo.com', port: 993 },
    custom: { server: '', port: 993 },
  };

  constructor() {
    this.sftpForm = new FormGroup({
      host_name: new FormControl('', [Validators.required]),
      port: new FormControl(22, [Validators.required, Validators.min(1), Validators.max(65535)]),
      username: new FormControl('', [Validators.required]),
      password: new FormControl(''),
      monitor_folders: new FormControl('', [Validators.required]),
      moved_folder: new FormControl('', [Validators.required]),
      failed_folder: new FormControl('', [Validators.required]),
      interval_minute: new FormControl(5, [Validators.required, Validators.min(1)]),
      is_active: new FormControl(false),
    });

    this.smtpForm = new FormGroup({
      email_id: new FormControl('', [Validators.required, Validators.email]),
      app_password: new FormControl('', [Validators.required]),
      approved_senders: new FormControl('', [Validators.required]),
      imap_server: new FormControl('imap.gmail.com', [Validators.required]),
      imap_port: new FormControl(993, [Validators.required]),
      interval_minute: new FormControl(5, [Validators.required, Validators.min(1)]),
      is_active: new FormControl(false),
    });
  }

  ngOnInit(): void {
    // Check if we're in edit mode from the route
    const editType = this.route.snapshot.paramMap.get('type');
    if (editType) {
      this.isEditMode = true;
      this.activeTab = editType.toUpperCase() as ConnectorTab;
    }

    this.loadExistingData();
  }

  ngOnDestroy(): void {
    this.unSubscribe$.next();
    this.unSubscribe$.complete();
  }

  loadExistingData(): void {
    this.apiService.get(`${URLS.userSftp}`).pipe(takeUntil(this.unSubscribe$)).subscribe({
      next: (res: any) => {
        if (res && res.sftp) {
          this.hasSftpData = true;
          this.sftpForm.patchValue({
            host_name: res.sftp.host_name,
            port: res.sftp.port,
            username: res.sftp.username,
            password: res.sftp.password || '',
            monitor_folders: res.sftp.monitor_folders,
            moved_folder: res.sftp.moved_folder,
            failed_folder: res.sftp.failed_folder,
            interval_minute: res.sftp.interval_minute,
            is_active: res.sftp.is_active,
          });
        }
      },
      error: () => {}
    });

    this.apiService.get(`${URLS.userSmtp}`).pipe(takeUntil(this.unSubscribe$)).subscribe({
      next: (res: any) => {
        if (res && res.smtp) {
          this.hasSmtpData = true;
          this.smtpForm.patchValue({
            email_id: res.smtp.email_id,
            app_password: res.smtp.app_password || '',
            approved_senders: res.smtp.approved_senders,
            imap_server: res.smtp.imap_server,
            imap_port: res.smtp.imap_port,
            interval_minute: res.smtp.interval_minute,
            is_active: res.smtp.is_active,
          });
          // Detect email method from imap server
          if (res.smtp.imap_server?.includes('gmail')) this.emailMethod = 'gmail';
          else if (res.smtp.imap_server?.includes('office365') || res.smtp.imap_server?.includes('outlook')) this.emailMethod = 'outlook';
          else if (res.smtp.imap_server?.includes('yahoo')) this.emailMethod = 'yahoo';
          else this.emailMethod = 'custom';
        }
      },
      error: () => {}
    });
  }

  setTab(tab: ConnectorTab): void {
    this.activeTab = tab;
  }

  setEmailMethod(method: string): void {
    this.emailMethod = method as EmailMethod;
    const defaults = this.imapDefaults[this.emailMethod];
    this.smtpForm.patchValue({
      imap_server: defaults.server,
      imap_port: defaults.port,
    });
  }

  onPemFileChange(event: Event): void {
    const input = event.target as HTMLInputElement;
    if (input.files && input.files.length > 0) {
      this.selectedPemFile = input.files[0];
    }
  }

  isSftpFieldInvalid(field: string): boolean {
    const ctrl = this.sftpForm.get(field);
    return !!(ctrl && ctrl.invalid && (ctrl.dirty || ctrl.touched));
  }

  isSmtpFieldInvalid(field: string): boolean {
    const ctrl = this.smtpForm.get(field);
    return !!(ctrl && ctrl.invalid && (ctrl.dirty || ctrl.touched));
  }

  onSftpSubmit(): void {
    if (this.sftpForm.invalid) {
      this.sftpForm.markAllAsTouched();
      return;
    }
    this.isSubmitting = true;
    const formData = new FormData();
    const val = this.sftpForm.value;
    formData.append('host_name', val.host_name);
    formData.append('port', val.port.toString());
    formData.append('username', val.username);
    if (val.password) formData.append('password', val.password);
    formData.append('monitor_folders', val.monitor_folders);
    formData.append('moved_folder', val.moved_folder);
    formData.append('failed_folder', val.failed_folder);
    formData.append('interval_minute', val.interval_minute.toString());
    formData.append('is_active', val.is_active.toString());
    if (this.selectedPemFile) {
      formData.append('pem_file', this.selectedPemFile);
    }

    if (this.hasSftpData) {
      this.apiService.put(`${URLS.userSftp}`, formData).pipe(takeUntil(this.unSubscribe$)).subscribe({
        next: () => { this.isSubmitting = false; this.router.navigate(['/main/connectors/list']); },
        error: () => { this.isSubmitting = false; }
      });
    } else {
      this.apiService.post(`${URLS.userSftp}`, formData).pipe(takeUntil(this.unSubscribe$)).subscribe({
        next: () => { this.isSubmitting = false; this.router.navigate(['/main/connectors/list']); },
        error: () => { this.isSubmitting = false; }
      });
    }
  }

  onSmtpSubmit(): void {
    if (this.smtpForm.invalid) {
      this.smtpForm.markAllAsTouched();
      return;
    }
    this.isSubmitting = true;
    const payload = { ...this.smtpForm.value };

    if (this.hasSmtpData) {
      this.apiService.put(`${URLS.userSmtp}`, payload).pipe(takeUntil(this.unSubscribe$)).subscribe({
        next: () => { this.isSubmitting = false; this.router.navigate(['/main/connectors/list']); },
        error: () => { this.isSubmitting = false; }
      });
    } else {
      this.apiService.post(`${URLS.userSmtp}`, payload).pipe(takeUntil(this.unSubscribe$)).subscribe({
        next: () => { this.isSubmitting = false; this.router.navigate(['/main/connectors/list']); },
        error: () => { this.isSubmitting = false; }
      });
    }
  }

  cancel(): void {
    this.router.navigate(['/main/connectors/list']);
  }
}
