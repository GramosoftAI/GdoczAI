import { HttpInterceptorFn } from '@angular/common/http';
import { inject } from '@angular/core';
import { finalize } from 'rxjs/operators';
import { LoaderService } from '../services/loader.service';

// Use a closure to maintain state
const createLoaderInterceptor = (): HttpInterceptorFn => {
  let requestCount = 0;

  return (req, next) => {
    const loaderService = inject(LoaderService);
    const skipLoader = req.headers.has('NoToStopLoader') ? false : true;

    if (!skipLoader) {
      requestCount++;
      if (requestCount === 1) {
        loaderService.show();
      }
    }

    return next(req).pipe(
      finalize(() => {
        if (!skipLoader) {
          requestCount--;
          if (requestCount === 0) {
            loaderService.hide();
          }
        }
      })
    );
  };
};

export const loaderInterceptor = createLoaderInterceptor();
