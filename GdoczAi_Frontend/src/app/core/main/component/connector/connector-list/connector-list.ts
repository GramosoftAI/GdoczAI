import { CommonModule } from '@angular/common';
import { Component, inject, OnDestroy, OnInit } from '@angular/core';
import { Router } from '@angular/router';
import { Subject, takeUntil } from 'rxjs';
import { ApiService } from '../../../../dependencies/services/api.service';
import { URLS } from '../../../../dependencies/config/api.config';

export interface IConnector {
  type: 'SFTP' | 'SMTP';
  identifier: string;
  hostOrEmail: string;
  intervalMin: number;
  isActive: boolean;
  rawData: any;
}

@Component({
  selector: 'app-connector-list',
  imports: [CommonModule],
  templateUrl: './connector-list.html',
  styleUrl: './connector-list.scss',
})
export class ConnectorList implements OnInit, OnDestroy {
  readonly apiService = inject(ApiService);
  readonly router = inject(Router);

  connectors: IConnector[] = [];
  isLoading = true;
  showDeleteModal = false;
  deletingConnector: IConnector | null = null;
  isDeleting = false;

  private unSubscribe$ = new Subject<void>();

  ngOnInit(): void {
    this.loadConnectors();
  }

  ngOnDestroy(): void {
    this.unSubscribe$.next();
    this.unSubscribe$.complete();
  }

  loadConnectors(): void {
    this.isLoading = true;
    this.connectors = [];
    let sftpDone = false;
    let smtpDone = false;

    const checkDone = () => {
      if (sftpDone && smtpDone) {
        this.isLoading = false;
      }
    };

    this.apiService.get(`${URLS.userSftp}`).pipe(takeUntil(this.unSubscribe$)).subscribe({
      next: (res: any) => {
        if (res && res.sftp) {
          this.connectors.push({
            type: 'SFTP',
            identifier: res.sftp.username || res.sftp.host_name,
            hostOrEmail: res.sftp.host_name,
            intervalMin: res.sftp.interval_minute,
            isActive: res.sftp.is_active,
            rawData: res.sftp,
          });
        }
        sftpDone = true;
        checkDone();
      },
      error: () => {
        sftpDone = true;
        checkDone();
      }
    });

    this.apiService.get(`${URLS.userSmtp}`).pipe(takeUntil(this.unSubscribe$)).subscribe({
      next: (res: any) => {
        if (res && res.smtp) {
          this.connectors.push({
            type: 'SMTP',
            identifier: res.smtp.email_id,
            hostOrEmail: res.smtp.email_id,
            intervalMin: res.smtp.interval_minute,
            isActive: res.smtp.is_active,
            rawData: res.smtp,
          });
        }
        smtpDone = true;
        checkDone();
      },
      error: () => {
        smtpDone = true;
        checkDone();
      }
    });
  }

  goToCreate(): void {
    this.router.navigate(['/main/connectors/create']);
  }

  editConnector(connector: IConnector): void {
    const type = connector.type.toLowerCase();
    this.router.navigate(['/main/connectors/edit', type]);
  }

  openDeleteModal(connector: IConnector): void {
    this.deletingConnector = connector;
    this.showDeleteModal = true;
  }

  closeDeleteModal(): void {
    this.showDeleteModal = false;
    this.deletingConnector = null;
  }

  confirmDelete(): void {
    if (!this.deletingConnector) return;
    this.isDeleting = true;
    const url = this.deletingConnector.type === 'SFTP' ? URLS.userSftp : URLS.userSmtp;
    this.apiService.delete(url).pipe(takeUntil(this.unSubscribe$)).subscribe({
      next: () => {
        this.isDeleting = false;
        this.closeDeleteModal();
        this.loadConnectors();
      },
      error: () => {
        this.isDeleting = false;
        this.closeDeleteModal();
      }
    });
  }
}
