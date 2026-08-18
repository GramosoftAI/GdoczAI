// components/toastr/toastr.component.ts
import { Component, OnInit, OnDestroy, ChangeDetectorRef } from '@angular/core';
import { CommonModule } from '@angular/common';
import { ToastrService, Toast } from '../../services/toastr.service';
import { Subject, takeUntil } from 'rxjs';

@Component({
  selector: 'app-toastr',
  standalone: true,
  imports: [CommonModule],
  templateUrl: './toastr.html',
  styleUrl: './toastr.scss'
})
export class Toastr implements OnInit, OnDestroy {
  toasts: Toast[] = [];
  positionClass = 'toastr-top-right';
  unSubscribe$ = new Subject<void>();

  constructor(
    private toastrService: ToastrService,
    private cdr: ChangeDetectorRef  // Add ChangeDetectorRef
  ) {}

  ngOnInit(): void {
    this.toastrService.getToasts().pipe(takeUntil(this.unSubscribe$)).subscribe((toasts: Toast[]) => {
      this.toasts = toasts;
      if (toasts.length > 0) {
        this.positionClass = `toastr-${toasts[0].position}`;
      }
      // Trigger change detection after receiving new toasts
      this.cdr.detectChanges();
    });
  }

  ngOnDestroy(): void {
    this.unSubscribe$.next();
    this.unSubscribe$.complete();
  }

  getToastClasses(toast: Toast): string {
    return `toastr-${toast.type}`;
  }

  removeToast(id: string): void {
    this.toastrService.remove(id);
  }

  getProgress(toast: Toast): number {
    return 100;
  }
}
