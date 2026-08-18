import { Component, EventEmitter, inject, Output } from '@angular/core';
import { ApiService } from '../../../dependencies/services/api.service';
import { URLS } from '../../../dependencies/config/api.config';
import { AuthRoutingModule } from "../../../auth/auth-routing-module";
import { Router, RouterModule } from '@angular/router';
import { DatePipe, DecimalPipe } from '@angular/common';

@Component({
  selector: 'app-usage',
  imports: [AuthRoutingModule, RouterModule, DecimalPipe, DatePipe],
  templateUrl: './usage.html',
  styleUrl: './usage.scss',
})
export class Usage {
  readonly apiService = inject(ApiService);
  readonly router = inject(Router)

  usages: any[] = [];
  parsedData: any[] = []

  ngOnInit(): void {
    this.getUserFiles();
  }

  getUserFiles(): void {
    const userData = sessionStorage.getItem('userDetails');
    const user = userData ? JSON.parse(userData) : null;
    const userId = user?.user_id;
    if (!userId) {
      return;
    }
    this.apiService.get(`${URLS.getFile}/user-files?user_id=${encodeURIComponent(String(userId))}`).subscribe((res) => {
      if (res && res.success) {
        this.usages = res.files;
      }
    });
  }

  goToPlayground(id: any) {
    this.router.navigate(['/main/playground', id]);
  }
}
