import { inject, Injectable } from '@angular/core';
import { CanActivate, ActivatedRouteSnapshot, RouterStateSnapshot, UrlTree, Router } from '@angular/router';
import { Observable } from 'rxjs';

@Injectable({
  providedIn: 'root'
})
export class AuthGuard implements CanActivate {
  readonly router = inject(Router);

  canActivate(
    next: ActivatedRouteSnapshot,
    state: RouterStateSnapshot
  ): Observable<boolean | UrlTree> | Promise<boolean | UrlTree> | boolean | UrlTree {

    const token = sessionStorage.getItem('token') || '';

    // If no token, redirect to login
    if (!token) {
      this.router.navigate(['/auth/sign_in']);
      return false;
    }

    // If token exists and user is trying to access auth routes, redirect to dashboard
    if (token && state.url.startsWith('/auth')) {
      this.router.navigate(['/main/dashboard']);
      return false;
    }

    // Allow access to protected routes
    return true;
  }
}
