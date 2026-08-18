// core/dependencies/interceptors/notification.interceptor.ts
import { HttpInterceptorFn } from '@angular/common/http';
import { inject } from '@angular/core';
import { ToastrService } from '../services/toastr.service';
import { tap } from 'rxjs';

export const notificationInterceptor: HttpInterceptorFn = (req, next) => {
  const toastr = inject(ToastrService);

  return next(req).pipe(
    tap({
      next: (event) => {
        if (event.type === 4) { // HttpResponse event type
          handleSuccessNotification(req, event, toastr);
        }
      },
      error: (error) => {
        handleErrorNotification(req, error, toastr);
      }
    })
  );
};

function handleSuccessNotification(req: any, res: any, toastr: ToastrService): void {
  const method = req.method.toLowerCase();
  const url = req.url;

  // Skip for GET requests or specific endpoints
  if (method === 'get' || url.includes('check') || url.includes('validate')) {
    return;
  }

  const message = getSuccessMessage(method, res.body);
  if (message) {
    toastr.success(message);
  }
}

function handleErrorNotification(req: any, error: any, toastr: ToastrService): void {
  const method = req.method.toLowerCase();

  // Get error message from response or use default
  const errorMessage = error.error?.message ||
                      error.error?.error ||
                      error.error?.detail ||
                      'Something went wrong!';

  toastr.error(getErrorMessage(method, errorMessage));
}

function getSuccessMessage(method: string, body: any): string | null {
  const messages: { [key: string]: string } = {
    post: body?.message || 'created successfully!',
    put: body?.message || 'updated successfully!',
    patch: body?.message || 'updated successfully!',
    delete: body?.message || 'deleted successfully!',
    login: 'Login successful!',
    logout: 'Logged out successfully!'
  };

  return messages[method] || null;
}

function getErrorMessage(method: string, errorMessage: string): string {
  // const actionMap: { [key: string]: string } = {
  //   post: 'creating',
  //   put: 'updating',
  //   patch: 'updating',
  //   delete: 'deleting',
  //   get: 'fetching',
  //   login: 'logging in'
  // };

  // const action = actionMap[method] || 'processing';
  return `${errorMessage}`;
}
