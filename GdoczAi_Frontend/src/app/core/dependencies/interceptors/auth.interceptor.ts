// auth.interceptor.ts
import { HttpInterceptorFn, HttpResponse } from '@angular/common/http';
import { inject } from '@angular/core';
import { catchError, map, throwError } from 'rxjs';
import { LoaderService } from '../services/loader.service';
import { AuthService } from '../services/auth.service';

export const authInterceptor: HttpInterceptorFn = (req, next) => {
  const loaderService = inject(LoaderService);
  const authService = inject(AuthService);

  let token = sessionStorage.getItem('token');
  let modifiedReq = req;

  if (!req.url.includes('')) {
    if (!req.headers.has('Content-Type')) {
      modifiedReq = req.clone({
        headers: req.headers.set('Content-Type', 'application/json')
      });
    }
  } else if (token) {
    const type = req.headers.get('Type');
    const isNoToStopLoader = req.headers.get('NoToStopLoader');

    modifiedReq = req.clone({
      setHeaders: {
        "Access-Control-Allow-Headers": "X-Requested-With",
        "Access-Control-Allow-Methods": "GET, POST, DELETE, PUT, OPTIONS",
        "Authorization": `Bearer ${token}`,
        "NoToStopLoader": `${isNoToStopLoader || 'false'}`,
        "Type": `${type ? type : 'RE'}`
      }
    });
  }

  return next(modifiedReq).pipe(
    map((event) => {
      if (event instanceof HttpResponse) {
        if (event.url && !event.url.includes('assets/data') && event.body) {
        }
      }
      return event;
    }),
    catchError((err) => {
      loaderService.hide();
      if (err.status === 401) {
        authService.logout();
      }
      return throwError(() => err);
    })
  );
};
