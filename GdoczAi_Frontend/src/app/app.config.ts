import { ApplicationConfig, importProvidersFrom, provideZonelessChangeDetection } from '@angular/core';
import { provideAnimationsAsync } from '@angular/platform-browser/animations/async';
import { provideRouter } from '@angular/router';
import { routes } from './app.routes';
import { provideHttpClient, withInterceptors } from '@angular/common/http';
import { authInterceptor } from './core/dependencies/interceptors/auth.interceptor';
import { loaderInterceptor } from './core/dependencies/interceptors/loader.interceptor';
import { notificationInterceptor } from './core/dependencies/interceptors/notification.interceptor';
// In your main.ts or app.config.ts
import 'bootstrap/dist/css/bootstrap.min.css';
import 'bootstrap/dist/js/bootstrap.bundle.js';

export const appConfig: ApplicationConfig = {
  providers: [
    provideZonelessChangeDetection(), // Keep zoneless
    provideRouter(routes),
    provideAnimationsAsync(),
    provideHttpClient(
      withInterceptors([authInterceptor, loaderInterceptor, notificationInterceptor])
    ),
  ]
};
