import { ChangeDetectorRef, Component, inject } from '@angular/core';
import { FormBuilder, FormGroup, Validators, FormArray, AbstractControl, FormControl, ReactiveFormsModule } from '@angular/forms';
import { Subject, takeUntil } from 'rxjs';
import { URLS } from '../../../../dependencies/config/api.config';
import { notOnlyWhitespace, noLeadingSpaceValidator } from '../../../../dependencies/directives/form-validation.directive';
import { ApiService } from '../../../../dependencies/services/api.service';
import { ToastrService } from '../../../../dependencies/services/toastr.service';
import { CommonModule } from '@angular/common';
import { ActivatedRoute, Params, Router, RouterModule } from '@angular/router';

@Component({
  selector: 'app-business-logic-form',
  imports: [CommonModule, ReactiveFormsModule, RouterModule],
  templateUrl: './business-logic-form.html',
  styleUrl: './business-logic-form.scss',
})
export class BusinessLogicForm {
  readonly fb = inject(FormBuilder);
  readonly apiService = inject(ApiService);
  readonly toastr = inject(ToastrService);
  readonly cdr = inject(ChangeDetectorRef);
  readonly activeRoute = inject(ActivatedRoute);
  readonly router = inject(Router)

  unSubscribe$ = new Subject<void>();

  showJsonOnClick: boolean = false;
  schemaForm: FormGroup;
  editingSchema: any = null;

  // JSON validation state
  hasJsonError = false;
  jsonError = '';
  id: string = ''



  constructor() {
    this.schemaForm = this.fb.group({
      business_logic_name: ['', [Validators.required, Validators.maxLength(30), notOnlyWhitespace(), noLeadingSpaceValidator()]],
      extraction_schema_text: ['', [notOnlyWhitespace(), noLeadingSpaceValidator()]],
      fields: this.fb.array([])
    });

    this.addDefaultField();
  }

  ngOnInit(): void {
    this.loadRouteParams();
    this.updateValidators();
  }

  private loadRouteParams(): void {
    this.activeRoute.paramMap.pipe(takeUntil(this.unSubscribe$)).subscribe((params: Params) => {
      this.id = params['params']?.id;
      if (this.id) {
        this.loadLogicById(this.id);
      }
    });
  }

  loadLogicById(id: string): void {
    this.apiService.get(`${URLS.docLogic}/${id}`).pipe(takeUntil(this.unSubscribe$)).subscribe((res: any) => {
      if (res && res.success) {
        this.editschema(res.logic)
      }
    });
  }

  ngOnDestroy(): void {
    this.unSubscribe$.next();
    this.unSubscribe$.complete();
  }

  private updateValidators(): void {
    const textControl = this.schemaForm.get('extraction_schema_text');

    textControl?.clearValidators();
    textControl?.setValidators([notOnlyWhitespace(), noLeadingSpaceValidator()])
    textControl?.updateValueAndValidity({ emitEvent: false });
  }

  // ===========================
  // JSON BUILDER
  // ===========================

  get fields(): FormArray {
    return this.schemaForm.get('fields') as FormArray;
  }

  toggleJsonView(): void {
    this.showJsonOnClick = !this.showJsonOnClick;
  }

  onPromptTypeChange(type: string): void {
    this.updateValidators();

    if (!this.schemaForm.contains('fields')) {
      this.schemaForm.addControl('fields', this.fb.array([]));
    }

    if (this.fields.length === 0) {
      this.addDefaultField();
    }
  }

  private addDefaultField(): void {
    if (this.fields.length === 0) {
      this.addField();
    }
  }

  formatJsonPayload(): any {
    const formattedFields = this.fields.value.map((field: any) => {
      const formattedField: any = {
        field_name: field.field_name || '',
        type: field.type || 'String',
        description: field.description || '',
        required: field.required !== undefined ? field.required : true
      };

      if (field.type === 'Object' && field.properties && field.properties.length > 0) {
        formattedField.properties = field.properties.map((prop: any) => ({
          field_name: prop.field_name || '',
          type: prop.type || 'String',
          description: prop.description || '',
          required: prop.required !== undefined ? prop.required : false
        }));
      }

      if (field.type === 'Array') {
        formattedField.items = {
          field_name: field.items?.field_name || 'item',
          type: field.items?.type || 'Object',
          description: field.items?.description || 'Array item schema',
          required: field.items?.required !== undefined ? field.items.required : true
        };

        if (field.items?.type === 'Object' && field.items?.properties && field.items.properties.length > 0) {
          formattedField.items.properties = field.items.properties.map((prop: any) => ({
            field_name: prop.field_name || '',
            type: prop.type || 'String',
            description: prop.description || '',
            required: prop.required !== undefined ? prop.required : false
          }));
        }
      }

      return formattedField;
    });

    return {
      fields: formattedFields
    };
  }

  private prepareFormData(): any {
    const formData = {
      business_logic_name: this.schemaForm.get('business_logic_name')?.value,
      business_logic_json: JSON.stringify(this.formatJsonPayload())
    };

    return formData;
  }

  onSubmit(): void {
    if (this.schemaForm.invalid) {
      this.markFormGroupTouched();
      return;
    }

    const formData = this.prepareFormData();

    try {
      JSON.stringify(formData.business_logic_json);
      this.hasJsonError = false;
      this.jsonError = '';
    } catch (error) {
      this.hasJsonError = true;
      this.jsonError = 'Invalid JSON structure';
      return;
    }

    if (this.editingSchema) {
      this.apiService.put(`${URLS.docLogic}/${this.editingSchema.logic_type_id}`, formData).pipe(takeUntil(this.unSubscribe$)).subscribe((res: any) => {
        if (res && res.success) {
          this.router.navigate(['/main/business-logic/list']);
          this.resetForm();
        }
      });
    } else {
      this.apiService.post(`${URLS.docLogic}`, formData).pipe(takeUntil(this.unSubscribe$)).subscribe((res: any) => {
        if (res) {
          this.router.navigate(['/main/business-logic/list']);
          this.resetForm();
        }
      });
    }
  }

  isFieldInvalid(fieldName: string): boolean {
    const field = this.schemaForm.get(fieldName);
    return !!(field && field.invalid && (field.dirty || field.touched));
  }

  editschema(data: any): void {
    this.resetForm();

    this.editingSchema = { ...data };

    this.schemaForm.patchValue({
      business_logic_name: data.logic_name || '',
      extraction_schema_text: ''
    });

    // Handle JSON mode
    try {
      let extractionSchemaValue: any;

      if (typeof data.logic_json === 'string') {
        extractionSchemaValue = JSON.parse(data.logic_json);
      } else {
        extractionSchemaValue = data.logic_json;
      }

      let fieldsArray: any[] = [];

      if (extractionSchemaValue && extractionSchemaValue.fields && Array.isArray(extractionSchemaValue.fields)) {
        fieldsArray = extractionSchemaValue.fields;
      } else if (Array.isArray(extractionSchemaValue)) {
        fieldsArray = extractionSchemaValue;
      } else if (extractionSchemaValue && typeof extractionSchemaValue === 'object') {
        fieldsArray = [extractionSchemaValue];
      }

      while (this.fields.length) {
        this.fields.removeAt(0);
      }

      if (fieldsArray && fieldsArray.length > 0) {
        setTimeout(() => {
          this.loadJsonToExtractForm(fieldsArray);
          this.fields.updateValueAndValidity();
          this.cdr.detectChanges()
        });
      } else {
        this.addDefaultField();
      }

    } catch (error) {
      console.error('Error parsing JSON schema:', error);
      this.toastr.error('Invalid JSON format in schema');

      this.addDefaultField();
    }
  }

  private loadJsonToExtractForm(fieldsData: any[]): void {
    while (this.fields.length) {
      this.fields.removeAt(0);
    }

    if (fieldsData && fieldsData.length > 0) {
      fieldsData.forEach((field: any) => {
        this.addField(
          field.field_name || '',
          field.type || 'String',
          field.description || '',
          field.required !== undefined ? field.required : true,
          field.properties || [],
          field.items || null
        );
      });

      // Force form to update
      this.fields.updateValueAndValidity();
    } else {
      // Add default field if no data
      this.addDefaultField();
    }
  }

  getSchemaById(data: any, type: string): void {
    this.apiService.get(`${URLS.docLogic}/${data.business_logic_name}`)
      .pipe(takeUntil(this.unSubscribe$))
      .subscribe((res: any) => {
        if (res && res.success) {
          this.schemaForm.patchValue(res)
        }
      });
  }

  // =============================================================
  // JSON BUILDER METHODS
  // =============================================================

  isArrayType(field: AbstractControl): boolean {
    const fieldGroup = this.getFieldGroup(field);
    return fieldGroup.get('type')?.value === 'Array';
  }

  getFieldTypeOptions(): string[] {
    return ['String', 'Number', 'Boolean', 'Object', 'Array'];
  }

  createField(
    field_name: string = '',
    type: string = 'String',
    description: string = '',
    required: boolean = true,
    properties?: any[],
    items?: any
  ): FormGroup {
    const fieldGroup = this.fb.group({
      field_name: [field_name, Validators.required],
      type: [type, Validators.required],
      description: [description],
      required: [required, Validators.required],
      properties: this.fb.array([]),
      items: this.fb.group({
        field_name: ['item'],
        type: ['Object'],
        description: ['Array item schema'],
        required: [true],
        properties: this.fb.array([])
      })
    });

    // Add properties if provided and type is Object
    if (properties && properties.length > 0 && type === 'Object') {
      const propertiesArray = fieldGroup.get('properties') as FormArray;
      properties.forEach(prop => {
        if (prop) {
          propertiesArray.push(this.createSubField(
            prop.field_name || '',
            prop.type || 'String',
            prop.description || '',
            prop.required !== undefined ? prop.required : false
          ));
        }
      });
    }

    // Add items if provided and type is Array
    if (items && type === 'Array') {
      const itemsGroup = fieldGroup.get('items') as FormGroup;
      itemsGroup.patchValue({
        field_name: items.field_name || 'item',
        type: items.type || 'Object',
        description: items.description || 'Array item schema',
        required: items.required !== undefined ? items.required : true
      });

      if (items.type === 'Object' && items.properties && items.properties.length > 0) {
        const itemPropertiesArray = itemsGroup.get('properties') as FormArray;
        items.properties.forEach((prop: any) => {
          if (prop) {
            itemPropertiesArray.push(this.createSubField(
              prop.field_name || '',
              prop.type || 'String',
              prop.description || '',
              prop.required !== undefined ? prop.required : false
            ));
          }
        });
      }
    }

    return fieldGroup;
  }

  getSubFields(fieldGroup: FormGroup): FormArray {
    const properties = fieldGroup.get('properties');
    return properties instanceof FormArray ? properties : new FormArray<any>([]);
  }

  createSubField(field_name: string = '', type: string = 'String', description: string = '', required: boolean = false): FormGroup {
    return this.fb.group({
      field_name: [field_name, Validators.required],
      type: [type, Validators.required],
      description: [description],
      required: [required]
    });
  }

  addSubField(fieldGroup: FormGroup): void {
    const subFields = this.getSubFields(fieldGroup);
    subFields.push(this.createSubField());
  }

  removeSubField(fieldGroup: FormGroup, index: number): void {
    const subFields = this.getSubFields(fieldGroup);
    if (subFields.length > 1) {
      subFields.removeAt(index);
    } else {
      this.toastr.warning('At least one property is required for Object type');
    }
  }

  addField(
    field_name?: string,
    type?: string,
    description?: string,
    required?: boolean,
    properties?: any[],
    items?: any
  ): void {
    const fieldGroup = this.createField(field_name, type, description, required, properties, items);
    this.fields.push(fieldGroup);
  }

  onTypeChange(fieldIndex: number): void {
    const fieldGroup = this.fields.at(fieldIndex) as FormGroup;
    const selectedType = fieldGroup.get('type')?.value;

    // Clear properties if type is not Object
    if (selectedType !== 'Object') {
      const propertiesArray = fieldGroup.get('properties') as FormArray;
      while (propertiesArray.length) {
        propertiesArray.removeAt(0);
      }
    }

    // Handle Array type - initialize with default structure
    if (selectedType === 'Array') {
      const itemsGroup = fieldGroup.get('items') as FormGroup;
      itemsGroup.patchValue({
        field_name: 'item',
        type: 'Object',
        description: 'Array item schema',
        required: true
      });

      // Initialize with one default property
      const itemProperties = itemsGroup.get('properties') as FormArray;
      while (itemProperties.length) {
        itemProperties.removeAt(0);
      }
      // Add default property
      itemProperties.push(this.createSubField('', 'String', '', false));
    }
  }

  getFieldGroup(field: AbstractControl): FormGroup {
    return field as FormGroup;
  }

  removeField(index: number): void {
    if (this.fields.length > 1) {
      this.fields.removeAt(index);
    } else {
      this.toastr.warning('At least one field is required');
    }
  }

  getValueTypeOptions(field?: AbstractControl): string[] {
    if (field && this.isArrayType(field)) {
      return ['String', 'Number', 'Boolean', 'Object'];
    }
    return ['String', 'Number', 'Boolean', 'Object'];
  }

  getPrimitiveTypeOptions(): string[] {
    return ['String', 'Number', 'Boolean'];
  }

  isArrayObjectType(fieldGroup: AbstractControl): boolean {
    const type = fieldGroup.get('type')?.value;
    const itemType = fieldGroup.get('items.type')?.value;

    return type === 'Array' && itemType === 'Object';
  }

  getItemsFormGroup(fieldGroup: FormGroup): FormGroup | null {
    const control = fieldGroup.get('items');
    return control instanceof FormGroup ? control : null;
  }

  getArrayItemProperties(fieldGroup: FormGroup): FormArray {
    const itemsGroup = this.getItemsFormGroup(fieldGroup);
    if (itemsGroup) {
      const properties = itemsGroup.get('properties');
      if (properties instanceof FormArray) {
        return properties;
      }
    }
    return new FormArray<any>([]);
  }

  // Add array item property
  addArrayItemProperty(fieldGroup: FormGroup): void {
    const properties = this.getArrayItemProperties(fieldGroup);
    properties.push(this.createSubField('', 'String', '', false));
  }

  // Remove array item property
  removeArrayItemProperty(fieldGroup: FormGroup, index: number): void {
    const properties = this.getArrayItemProperties(fieldGroup);
    if (properties.length > 1) {
      properties.removeAt(index);
    } else {
      this.toastr.warning('At least one property is required for Array items');
    }
  }

  // Method to get subfields as FormGroup array for template
  getSubFieldsForTemplate(fieldGroup: FormGroup): FormGroup[] {
    return this.getSubFields(fieldGroup).controls as FormGroup[];
  }

  // Method to get array item properties as FormGroup array for template
  getArrayItemPropertiesForTemplate(fieldGroup: FormGroup): FormGroup[] {
    return this.getArrayItemProperties(fieldGroup).controls as FormGroup[];
  }

  // Method to get subfields count
  getSubFieldsCount(fieldGroup: FormGroup): number {
    return this.getSubFields(fieldGroup).length;
  }

  // Method to get array item properties count
  getArrayItemPropertiesCount(fieldGroup: FormGroup): number {
    return this.getArrayItemProperties(fieldGroup).length;
  }

  onArrayItemPropertyTypeChange(fieldGroup: FormGroup, index: number): void {
    if (index === 0) {
      const properties = this.getArrayItemProperties(fieldGroup);
      const firstPropType = properties.at(0)?.get('type')?.value;
      if (firstPropType !== 'Object') {
        while (properties.length > 1) {
          properties.removeAt(1);
        }
      }
    }
  }

  // Get form control safely
  // Add this helper method to safely get form controls
  getFormControl(group: FormGroup, controlName: string): FormControl {
    const control = group.get(controlName);
    if (!(control instanceof FormControl)) {
      return new FormControl('');
    }
    return control;
  }

  getFormGroup(control: AbstractControl): FormGroup {
    if (!(control instanceof FormGroup)) {
      // Create and return a new empty FormGroup if control is not a FormGroup
      return this.fb.group({});
    }
    return control;
  }

  // =============================================================


  resetForm(): void {
    this.schemaForm.reset({
      business_logic_name: '',
      extraction_schema_text: ''
    });

    // Clear and reinitialize fields array
    while (this.fields.length) {
      this.fields.removeAt(0);
    }
    this.addDefaultField();

    this.showJsonOnClick = false;
    this.hasJsonError = false;
    this.jsonError = '';
    this.editingSchema = null;

    // Reset validators for text field based on current prompt type
    this.updateValidators();

    // Mark as pristine/untouched
    this.schemaForm.markAsPristine();
    this.schemaForm.markAsUntouched();
  }

  private markFormGroupTouched(): void {
    Object.values(this.schemaForm.controls).forEach(control => {
      control.markAsTouched();

      if (control instanceof FormGroup) {
        Object.values(control.controls).forEach(c => c.markAsTouched());
      }

      if (control instanceof FormArray) {
        control.controls.forEach(c => {
          if (c instanceof FormGroup) {
            Object.values(c.controls).forEach(cc => cc.markAsTouched());
          }
        });
      }
    });
  }

}
