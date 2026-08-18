import { Component, inject } from '@angular/core';
import { Router } from '@angular/router';
import { Subject, takeUntil } from 'rxjs';
import { URLS } from '../../../../dependencies/config/api.config';
import { ApiService } from '../../../../dependencies/services/api.service';
import { ToastrService } from '../../../../dependencies/services/toastr.service';
import { JsonFormatPipe } from "../../../../dependencies/pipes/json-format.pipe";
import { ReactiveFormsModule } from '@angular/forms';
import { CommonModule } from '@angular/common';

@Component({
  selector: 'app-business-logic-list',
  imports: [CommonModule , ReactiveFormsModule, JsonFormatPipe],
  templateUrl: './business-logic-list.html',
  styleUrl: './business-logic-list.scss',
})
export class BusinessLogicList {
  readonly apiService = inject(ApiService);
  public router = inject(Router);
  readonly toastr = inject(ToastrService);

  Schemas: any[] = [];
  unSubscribe$ = new Subject<void>();

  // Modal state
  showDeleteModal = false;
  typeToDelete: any = {
    logic_type_id: '',
    logic_name: '',
    logic_json: ''
  };

  ngOnInit() {
    this.getAllLogics();
  }

  getAllLogics(): void {
    this.apiService.get(`${URLS.docLogic}`).pipe(takeUntil(this.unSubscribe$)).subscribe((res: any) => {
      if (res && res.success) {
        this.Schemas = res.logics || [];
      }
    });
  }

  confirmDelete(data: any): void {
    this.typeToDelete = { ...data };
    this.showDeleteModal = true;
  }

  closeDeleteModal(): void {
    this.showDeleteModal = false;
    this.typeToDelete = {
      logic_type_id: '',
      logic_name: '',
      logic_json: '',
    };
  }

  deleteType(): void {
    if (this.typeToDelete.logic_type_id) {
      this.apiService.delete(`${URLS.docLogic}/${this.typeToDelete.logic_type_id}`).pipe(takeUntil(this.unSubscribe$)).subscribe((res: any) => {
        if (res && res.success) {
          this.closeDeleteModal();
          this.getAllLogics();
        }
      });
    }
  }

}
