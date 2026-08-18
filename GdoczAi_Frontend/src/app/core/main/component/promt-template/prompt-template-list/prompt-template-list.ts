import { Component, inject } from '@angular/core';
import { Subject, takeUntil } from 'rxjs';
import { URLS } from '../../../../dependencies/config/api.config';
import { ApiService } from '../../../../dependencies/services/api.service';
import { JsonFormatPipe } from "../../../../dependencies/pipes/json-format.pipe";
import { Router } from '@angular/router';
import { ToastrService } from '../../../../dependencies/services/toastr.service';

@Component({
  selector: 'app-prompt-template-list',
  imports: [JsonFormatPipe],
  templateUrl: './prompt-template-list.html',
  styleUrl: './prompt-template-list.scss',
})
export class PromptTemplateList {
  readonly apiService = inject(ApiService);
  public router = inject(Router);
  readonly toastr = inject(ToastrService);

  Schemas: any[] = [];
  unSubscribe$ = new Subject<void>();

  // Modal state
  showDeleteModal = false;
  typeToDelete: any = {
    id: '',
    doc_type_id: '',
    extraction_schema: '',
    business_logic: ''
  };

  ngOnInit() {
    this.getAllSchemas();
  }

  getAllSchemas(): void {
    this.apiService.get(`${URLS.schema}`).pipe(takeUntil(this.unSubscribe$)).subscribe((res: any) => {
      if (res && res.success) {
        this.Schemas = res.schemas || [];
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
      id: '',
      doc_type_id: '',
      extraction_schema: '',
      business_logic: 'text'
    };
  }

  deleteType(): void {
    if (this.typeToDelete.id) {
      this.apiService.delete(`${URLS.schema}/${this.typeToDelete.id}`).pipe(takeUntil(this.unSubscribe$)).subscribe((res: any) => {
        if (res && res.success) {
          this.closeDeleteModal();
          this.getAllSchemas();
          // this.toastr.success('Schema deleted successfully');
        }
      });
    }
  }

}
