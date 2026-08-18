// services/toastr.service.ts
import { Injectable, Inject, PLATFORM_ID } from '@angular/core';
import { isPlatformBrowser } from '@angular/common';
import { BehaviorSubject, Observable } from 'rxjs';

export type ToastType = 'success' | 'error' | 'warning' | 'info';

export interface ToastConfig {
  message: string;
  type: ToastType;
  duration?: number;
  position?: 'top-right' | 'top-left' | 'bottom-right' | 'bottom-left';
}

export interface Toast {
  id: string;
  message: string;
  type: ToastType;
  duration: number;
  position: string;
  visible: boolean;
}

@Injectable({
  providedIn: 'root'
})
export class ToastrService {
  private toasts: Toast[] = [];
  private toastsSubject = new BehaviorSubject<Toast[]>([]);
  private defaultDuration = 3000;
  private defaultPosition = 'top-right';
  private timeoutIds = new Map<string, ReturnType<typeof setTimeout>>();

  constructor(@Inject(PLATFORM_ID) private platformId: any) { }

  show(config: ToastConfig): void {
    if (!isPlatformBrowser(this.platformId)) {
      return;
    }

    const duration = config.duration || this.defaultDuration;

    const toast: Toast = {
      id: Date.now().toString() + Math.random().toString(36).substr(2, 9),
      message: config.message,
      type: config.type,
      duration: duration,
      position: config.position || this.defaultPosition,
      visible: true
    };

    this.toasts.push(toast);
    this.toastsSubject.next([...this.toasts]);

    if (duration > 0) {
      const timeoutId = window.setTimeout(() => {
        this.remove(toast.id);
      }, duration);

      this.timeoutIds.set(toast.id, timeoutId);
    }
  }

  success(message: string, duration?: number): void {
    this.show({
      message,
      type: 'success',
      duration: duration || 3000
    });
  }

  error(message: string, duration?: number): void {
    this.show({
      message,
      type: 'error',
      duration: duration || 3000
    });
  }

  warning(message: string, duration?: number): void {
    this.show({
      message,
      type: 'warning',
      duration: duration || 5000
    });
  }

  info(message: string, duration?: number): void {
    this.show({
      message,
      type: 'info',
      duration: duration || 4000
    });
  }

  remove(id: string): void {
    const timeoutId = this.timeoutIds.get(id);
    if (timeoutId) {
      window.clearTimeout(timeoutId);
      this.timeoutIds.delete(id);
    }

    const toastIndex = this.toasts.findIndex(t => t.id === id);

    if (toastIndex > -1) {
      this.toasts[toastIndex].visible = false;
      this.toastsSubject.next([...this.toasts]);

      setTimeout(() => {
        this.toasts = this.toasts.filter(t => t.id !== id);
        this.toastsSubject.next([...this.toasts]);
      }, 300);
    }
  }

  clear(): void {
    this.timeoutIds.forEach((timeoutId, id) => {
      clearTimeout(timeoutId);
    });
    this.timeoutIds.clear();

    this.toasts.forEach(toast => {
      toast.visible = false;
    });
    this.toastsSubject.next([...this.toasts]);

    setTimeout(() => {
      this.toasts = [];
      this.toastsSubject.next([]);
    }, 300);
  }

  getToasts(): Observable<Toast[]> {
    return this.toastsSubject.asObservable();
  }

  destroy(): void {
    this.timeoutIds.forEach((timeoutId) => {
      clearTimeout(timeoutId);
    });
    this.timeoutIds.clear();
    this.toasts = [];
    this.toastsSubject.next([]);
  }
}
