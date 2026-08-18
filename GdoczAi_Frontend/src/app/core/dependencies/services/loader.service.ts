import { Injectable } from '@angular/core';
import { BehaviorSubject, Observable } from 'rxjs';

@Injectable({
  providedIn: 'root'
})
export class LoaderService {
  private loading$ = new BehaviorSubject<boolean>(false);
  private activeRequests = 0;

  get isLoading$(): Observable<boolean> {
    return this.loading$.asObservable();
  }

  show(): void {
    this.activeRequests++;
    if (this.activeRequests === 1) {
      this.loading$.next(true);
    }
  }

  hide(): void {
    this.activeRequests--;
    if (this.activeRequests <= 0) {
      this.activeRequests = 0;
      this.loading$.next(false);
    }
  }

  reset(): void {
    this.activeRequests = 0;
    this.loading$.next(false);
  }
}
