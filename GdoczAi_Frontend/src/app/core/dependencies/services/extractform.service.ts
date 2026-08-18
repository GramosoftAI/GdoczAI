import { inject, Injectable } from '@angular/core';
import {
  AbstractControl, FormArray, FormBuilder,
  FormControl, FormGroup, Validators
} from '@angular/forms';
import { ToastrService } from './toastr.service';

@Injectable({ providedIn: 'root' })
export class ExtractFormService {
  private readonly fb = inject(FormBuilder);
  private readonly toastr = inject(ToastrService);

  // ── Field type options ───────────────────────────────────────────────────────

  getFieldTypeOptions(): string[] { return ['String', 'Number', 'Boolean', 'Object', 'Array']; }
  getPrimitiveTypeOptions(): string[] { return ['String', 'Number', 'Boolean']; }
  getValueTypeOptions(field: AbstractControl): string[] {
    return this.isArrayType(field)
      ? ['String', 'Number', 'Boolean', 'Object']
      : ['String', 'Number', 'Boolean'];
  }

  // ── Type guards ──────────────────────────────────────────────────────────────

  isArrayType(field: AbstractControl): boolean {
    return this.asGroup(field).get('type')?.value === 'Array';
  }

  isArrayObjectType(field: AbstractControl): boolean {
    return field.get('type')?.value === 'Array' && field.get('items.type')?.value === 'Object';
  }

  // ── FormGroup factories ──────────────────────────────────────────────────────

  createField(name = '', type = 'String', desc = '', req = true): FormGroup {
    const cfg: any = {
      field_name: new FormControl(name, Validators.required),
      type: new FormControl(type, Validators.required),
      description: new FormControl(desc),
      required: new FormControl(req, Validators.required)
    };
    if (type === 'Object') {
      cfg.properties = this.fb.array([this.createSubField()]);
    }
    if (type === 'Array') {
      const ig = this.fb.group(this.createField('item', 'String', 'Array item schema', true).controls);
      ig.addControl('properties', this.fb.array([this.createSubField()]));
      cfg.items = ig;
    }
    return this.fb.group(cfg);
  }

  createSubField(name = '', type = 'String', desc = '', req = true): FormGroup {
    return this.fb.group({
      field_name: new FormControl(name, Validators.required),
      type: new FormControl(type, Validators.required),
      description: new FormControl(desc),
      required: new FormControl(req)
    });
  }

  buildExtractForm(): FormGroup {
    return this.fb.group({ doc_type_id: [''], fields: this.fb.array<FormGroup>([]) });
  }

  // ── Field array management ───────────────────────────────────────────────────

  addField(
    fields: FormArray, mode: string,
    name?: string, type?: string, desc?: string, req?: boolean
  ): void {
    if (mode === 'demo' && fields.length >= 5) {
      this.toastr.error('Please log in first to add more than 5 fields.'); return;
    }
    fields.push(this.createField(name, type, desc, req));
    this.ensureControls(fields);
  }

  removeField(fields: FormArray, i: number): void {
    fields.length > 1
      ? fields.removeAt(i)
      : this.toastr.warning('At least one field is required');
  }

  // ── Sub-field management (Object properties) ─────────────────────────────────

  getSubFields(g: FormGroup): FormArray {
    return g.get('properties') as FormArray;
  }

  addSubField(g: FormGroup): void {
    this.getSubFields(g)?.push(this.createSubField());
  }

  removeSubField(g: FormGroup, i: number): void {
    const a = this.getSubFields(g);
    a?.length > 1
      ? a.removeAt(i)
      : this.toastr.warning('At least one subfield is required for Object type');
  }

  // ── Array item properties ────────────────────────────────────────────────────

  getItemsFormGroup(g: FormGroup): FormGroup | null {
    const c = g.get('items'); return c instanceof FormGroup ? c : null;
  }

  getArrayItemProperties(g: FormGroup): FormArray | null {
    const ig = this.getItemsFormGroup(g);
    if (!ig) return null;
    const p = ig.get('properties');
    return p instanceof FormArray ? p : null;
  }

  addArrayItemProperty(g: FormGroup): void {
    this.getArrayItemProperties(g)?.push(this.createSubField());
  }

  removeArrayItemProperty(g: FormGroup, i: number): void {
    const a = this.getArrayItemProperties(g);
    a && a.length > 1 ? a.removeAt(i) : this.toastr.warning('At least one property is required for Array items');
  }

  // ── Type change handlers ─────────────────────────────────────────────────────

  onTypeChange(fields: FormArray, idx: number): void {
    const g = fields.at(idx) as FormGroup;
    const type = g.get('type')?.value;

    if (type !== 'Object' && g.contains('properties')) g.removeControl('properties');
    if (type !== 'Array' && g.contains('items')) g.removeControl('items');

    if (type === 'Object' && !g.contains('properties'))
      g.addControl('properties', this.fb.array([this.createSubField()]));

    if (type === 'Array' && !g.contains('items')) {
      const ig = this.fb.group(this.createField('item', 'String', 'Array item schema', true).controls);
      ig.addControl('properties', this.fb.array([this.createSubField()]));
      g.addControl('items', ig);
    }
  }

  onArrayItemTypeChange(fields: FormArray, idx: number): void {
    const ig = (fields.at(idx) as FormGroup).get('items') as FormGroup;
    if (!ig) return;
    const type = ig.get('type')?.value;
    if (type === 'Object' && !ig.contains('properties'))
      ig.addControl('properties', this.fb.array([this.createSubField()]));
    else if (type !== 'Object' && ig.contains('properties'))
      ig.removeControl('properties');
  }

  ensureControls(fields: FormArray): void {
    fields.controls.forEach(ctrl => {
      const g = ctrl as FormGroup;
      const type = g.get('type')?.value;
      if (type === 'Object' && !g.contains('properties'))
        g.addControl('properties', this.fb.array([this.createSubField()]));
      if (type === 'Array' && !g.contains('items')) {
        const ig = this.fb.group(this.createField('item', 'String', 'Array item schema', true).controls);
        ig.addControl('properties', this.fb.array([this.createSubField()]));
        g.addControl('items', ig);
      }
    });
  }

  // ── Control accessors ────────────────────────────────────────────────────────

  asGroup(f: AbstractControl): FormGroup {
    return f instanceof FormGroup ? f : this.fb.group({});
  }

  asControl(g: FormGroup, name: string): FormControl {
    const c = g.get(name); return c instanceof FormControl ? c : new FormControl('');
  }

  markAllTouched(fields: FormArray): void {
    fields.controls.forEach(ctrl => {
      if (ctrl instanceof FormGroup)
        Object.values(ctrl.controls).forEach(c => c.markAsTouched());
    });
  }

  // ── Payload formatting ───────────────────────────────────────────────────────

  formatPayload(formValue: any): any {
    if (!formValue?.fields) return formValue;
    return {
      ...formValue,
      fields: formValue.fields.map((f: any) => {
        const out: any = {
          field_name: f.field_name, type: f.type,
          description: f.description, required: f.required
        };
        if (f.type === 'Object' && f.properties)
          out.properties = f.properties.map((p: any) =>
            ({ field_name: p.field_name, type: p.type, description: p.description, required: p.required })
          );
        if (f.type === 'Array' && f.items) {
          out.items = {
            field_name: f.items.field_name ?? 'item',
            type: f.items.type ?? 'Object',
            description: f.items.description ?? '',
            required: f.items.required ?? true
          };
          if (f.items.type === 'Object' && f.items.properties)
            out.items.properties = f.items.properties.map((p: any) =>
              ({ field_name: p.field_name, type: p.type, description: p.description, required: p.required })
            );
        }
        return out;
      })
    };
  }

  // ── Schema loader ────────────────────────────────────────────────────────────

  parseSchemaFields(extraction_schema: any): any[] | null {
    try {
      const data = typeof extraction_schema === 'string'
        ? JSON.parse(extraction_schema)
        : extraction_schema;
      const fields: any[] = data?.fields && Array.isArray(data.fields)
        ? data.fields
        : Array.isArray(data) ? data : [data];
      return fields.length ? fields : null;
    } catch { return null; }
  }

  loadFieldsIntoArray(fields: FormArray, mode: string, fieldsData: any[]): void {
    while (fields.length) fields.removeAt(0);

    fieldsData.forEach(f => {
      this.addField(fields, mode, f.field_name ?? '', f.type ?? 'String', f.description ?? '', f.required ?? true);
      const g = fields.at(fields.length - 1) as FormGroup;

      if (f.type === 'Object' && f.properties?.length) {
        const arr = g.get('properties') as FormArray;
        if (arr) {
          while (arr.length) arr.removeAt(0);
          f.properties.forEach((p: any) =>
            arr.push(this.createSubField(p.field_name ?? '', p.type ?? 'String', p.description ?? '', p.required ?? false))
          );
        }
      }

      if (f.type === 'Array' && f.items) {
        const ig = g.get('items') as FormGroup;
        if (ig) {
          ig.patchValue({
            field_name: f.items.field_name ?? 'item',
            type: f.items.type ?? 'Object',
            description: f.items.description ?? '',
            required: f.items.required ?? true
          });
          if (f.items.type === 'Object' && f.items.properties?.length) {
            if (!ig.contains('properties')) ig.addControl('properties', this.fb.array([]));
            const ip = ig.get('properties') as FormArray;
            while (ip.length) ip.removeAt(0);
            f.items.properties.forEach((p: any) =>
              ip.push(this.createSubField(p.field_name ?? '', p.type ?? 'String', p.description ?? '', p.required ?? false))
            );
          }
        }
      }
    });

    fields.updateValueAndValidity();
  }

  // ── Extraction result helpers ────────────────────────────────────────────────

  getDataSections(extractedData: any): any[] {
    if (!extractedData || typeof extractedData !== 'object') return [];

    const sections: any[] = [];
    const flatItems: any[] = [];

    Object.entries(extractedData).forEach(([key, value]) => {
      if (value === null || typeof value !== 'object') {
        flatItems.push({ key, value: String(value ?? '-') });
      } else {
        sections.push({
          title: key,
          items: this.parseData(value)
        });
      }
    });

    if (flatItems.length > 0) {
      sections.unshift({ title: 'Extracted Data', items: flatItems });
    }
    return sections;
  }

  parseData(data: any): any[] {
    if (!data) return [];

    if (Array.isArray(data)) {
      return data.map((v, i) => ({
        key: String(i + 1),
        value: typeof v === 'object' ? JSON.stringify(v) : String(v)
      }));
    }

    if (typeof data === 'object') {
      return Object.entries(data).map(([k, v]) => ({
        key: k,
        value: typeof v === 'object' ? JSON.stringify(v) : String(v)
      }));
    }

    const str = String(data);
    if (str.includes(':')) {
      return str.split('\n').filter(l => l.includes(':')).map(l => {
        const [key, ...val] = l.split(':');
        return { key: key.trim(), value: val.join(':').trim() };
      });
    }

    return [{ key: 'Result', value: str }];
  }
}