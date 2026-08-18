import { Injectable } from '@angular/core';

@Injectable({
  providedIn: 'root'
})
export class LocalStorageService {

  constructor() { }

  public setAccessToken(token: string): void {
    sessionStorage.setItem('token', token);
  }

  public getAccessToken(): string {
    return sessionStorage.getItem('token')!;
  }

  public setUserDetails(data: any): void {
    sessionStorage.setItem('userDetails', JSON.stringify(data));
  }

  public getUserDetails(): any {
    return JSON.parse(sessionStorage.getItem('userDetails') || '{}')!;
  }

  public clearSessionStore(): void {
    sessionStorage.clear();
  }
}
