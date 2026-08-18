import { CommonModule } from '@angular/common';
import { ChangeDetectorRef, Component, ElementRef, inject, ViewChild } from '@angular/core';
import { ApiService } from '../../../dependencies/services/api.service';
import { URLS } from '../../../dependencies/config/api.config';
import { Subject, takeUntil } from 'rxjs';
import { ToastrService } from '../../../dependencies/services/toastr.service';
import { Modal } from 'bootstrap';



@Component({
  selector: 'app-api-keys',
  imports: [CommonModule],
  templateUrl: './api-keys.html',
  styleUrl: './api-keys.scss',
})
export class ApiKeys {
  readonly apiService = inject(ApiService);
  readonly toastr = inject(ToastrService);
  readonly cdr = inject(ChangeDetectorRef)

  @ViewChild('confirmationModal') modalRef!: ElementRef;
  modalInstance!: Modal;

  unSubscribe$ = new Subject<void>();
  apiKeyList: any;
  apiKey: any;

  constructor() { }

  ngOnInit() {
    this.getKeyList();
  }

  ngAfterViewInit() {
    this.modalInstance = new Modal(this.modalRef.nativeElement);
  }


  getKeyList() {
    this.apiService.get(URLS.apiKeys).pipe(takeUntil(this.unSubscribe$)).subscribe({
      next: (res: any) => {
        if (res.success == true) {
          this.apiKeyList = res.api_key
        }
      },
      error: (err: any) => {
        this.toastr.error(err);
      }
    })
  }

  createKeyList() {
  this.apiService.post(URLS.apiKeys, {})
    .pipe(takeUntil(this.unSubscribe$))
    .subscribe({
      next: (res: any) => {
        this.apiKey = res.api_key;
        this.cdr.detectChanges();

        this.modalInstance.show();
        this.getKeyList();
      }
    });
}



  deleteKey() {
    this.apiService.delete(URLS.apiKeys).pipe(takeUntil(this.unSubscribe$)).subscribe({
      next: (res: any) => {
        if (res) {
          // this.toastr.success(res.message);
          this.getKeyList();
        }
      },
      error: (err: any) => {
        this.toastr.error(err);
      }
    })
  }

  copyToClipboard(text: string) {
  if (!text) return;

  navigator.clipboard.writeText(text)
    .then(() => {
      this.toastr.success('API Key copied to clipboard!');
    })
    .catch(() => {
      this.toastr.error('Failed to copy API Key');
    });
}


  closeModal() {
    this.modalInstance.hide();
  }
}
