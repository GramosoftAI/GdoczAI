// spinner.component.ts
import { Component, inject, OnDestroy } from '@angular/core';
import { Observable, Subject, takeUntil } from 'rxjs';
import { LoaderService } from '../../services/loader.service';
import { CommonModule } from '@angular/common';

@Component({
  selector: 'app-spinner',
  standalone: true,
  imports: [CommonModule],
  templateUrl: './spinner.html',
  styleUrl: './spinner.scss',
})
export class Spinner implements OnDestroy {
  private destroy$ = new Subject<void>();
  private loaderService = inject(LoaderService);

  showLoader$: Observable<boolean>;

  constructor() {
    this.showLoader$ = this.loaderService.isLoading$;
  }

  ngOnDestroy(): void {
    this.destroy$.next();
    this.destroy$.complete();
  }
}
