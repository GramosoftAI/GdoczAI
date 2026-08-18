import { inject, Injectable } from '@angular/core';
import { Router } from '@angular/router';
import { LocalStorageService } from './local-storage.service';

@Injectable({
  providedIn: 'root'
})

export class AuthService {
  readonly localService = inject(LocalStorageService);
  readonly router = inject(Router);

  isAuthenticated: boolean = this.localService.getAccessToken() ? true : false;

  constructor() { }

  login(data: any) {
    this.localService.setAccessToken(data.access_token);
    this.localService.setUserDetails(data.user);
    // this.router.navigate(['/main/dashboard']);
  }

  logout() {
    this.localService.clearSessionStore();
    this.router.navigate(['/auth/sign_in']);
  }

  isTokenExpired(token?: string): boolean {
    if (!token) token = this.localService.getAccessToken();
    if (!token) return true;
    return false;
  }
}
