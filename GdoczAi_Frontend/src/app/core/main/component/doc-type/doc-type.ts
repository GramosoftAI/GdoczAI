import { Component, inject } from '@angular/core';
import { FormBuilder, FormGroup, ReactiveFormsModule, Validators } from '@angular/forms';
import { CommonModule } from '@angular/common';
import { URLS } from '../../../dependencies/config/api.config';
import { Subject, takeUntil } from 'rxjs';
import { ApiService } from '../../../dependencies/services/api.service';
import { noLeadingSpaceValidator, notOnlyWhitespace } from '../../../dependencies/directives/form-validation.directive';

@Component({
  selector: 'app-doc-type',
  standalone: true,
  imports: [ReactiveFormsModule, CommonModule],
  templateUrl: './doc-type.html',
  styleUrls: ['./doc-type.scss']
})
export class DocType {
  readonly fb = inject(FormBuilder);
  readonly apiService = inject(ApiService);

  unSubscribe$ = new Subject();

  documentForm: FormGroup;
  editingDoc: any = null;
  docTypes: any[] = [];

  // Modal state
  showDeleteModal = false;
  typeToDelete: any = {
    id: '',
    document_type: '',
    conditional_keys: '',
    langchain_keys: ''

  };

  constructor() {
    this.documentForm = this.fb.group({
      document_type: ['', [ Validators.required, notOnlyWhitespace(), noLeadingSpaceValidator()]],
      conditional_keys: ['', [notOnlyWhitespace(), noLeadingSpaceValidator()]],
      langchain_keys: ['', [notOnlyWhitespace(), noLeadingSpaceValidator()]]
    });
  }

  ngOnInit(): void {
    this.getAllDocTypes();
  }

  getAllDocTypes(): void {
    this.apiService.get(`${URLS.docType}`).pipe(takeUntil(this.unSubscribe$)).subscribe((res: any) => {
      if (res && res.success) {
        this.docTypes = res.document_types;
      };
    })
  }

  isFieldInvalid(fieldName: string): boolean {
    const field = this.documentForm.get(fieldName);
    return !!(field && field.invalid && (field.dirty || field.touched));
  }

  onSubmit() {
    if (this.documentForm.valid) {
      const formValue = this.documentForm.value;

      if (this.editingDoc) {
        this.apiService.put(`${URLS.docType}/${this.editingDoc.doc_type_id}`, formValue).pipe(takeUntil(this.unSubscribe$)).subscribe((res: any) => {
          if (res && res.success) {
            this.getAllDocTypes();
          }
        })
      } else {
        this.apiService.post(`${URLS.docType}`, formValue).pipe(takeUntil(this.unSubscribe$)).subscribe((res: any) => {
          if (res) {
            this.getAllDocTypes();
          }
        })
      }

      this.cancelEdit();
    } else {
      this.markFormGroupTouched();
    }
  }

  editType(data: any) {
    this.editingDoc = { ...data };
    this.documentForm.patchValue({
      document_type: data.document_type,
      conditional_keys: data.conditional_keys,
      langchain_keys: data.langchain_keys
    });
  }

  confirmDelete(data: any) {
    this.typeToDelete = { ...data };
    this.showDeleteModal = true;
  }

  getDocById(data: any, type: string) {
    this.apiService.get(`${URLS.docType}/${data.doc_type_id}`).pipe(takeUntil(this.unSubscribe$)).subscribe((res: any) => {
      if (res && res.success) {
        if (type === 'edit') {
          this.editType(data);
        } else if (type === 'delete') {
          this.confirmDelete(data);
        }
      }
    })
  }

  deleteType() {
    if (this.typeToDelete.doc_type_id) {
      this.apiService.delete(`${URLS.docType}/${this.typeToDelete.doc_type_id}`, '').pipe(takeUntil(this.unSubscribe$)).subscribe((res: any) => {
        if (res && res.success) {
          this.closeDeleteModal();
          this.getAllDocTypes();
        }
      })
    }
  }

  closeDeleteModal() {
    this.showDeleteModal = false;
    this.typeToDelete = {
      id: '',
      document_type: '',
      conditional_keys: '',
      langchain_keys: ''
    };
  }

  cancelEdit() {
    this.editingDoc = null;
    this.resetForm();
  }

  resetForm() {
    this.documentForm.reset({
      document_type: '',
      conditional_keys: '',
      langchain_keys: ''
    });
    // Reset validation states
    this.documentForm.markAsPristine();
    this.documentForm.markAsUntouched();
  }

  private markFormGroupTouched() {
    Object.keys(this.documentForm.controls).forEach(key => {
      const control = this.documentForm.get(key);
      control?.markAsTouched();
    });
  }
}
