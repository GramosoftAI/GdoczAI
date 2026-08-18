import { CommonModule } from '@angular/common';
import {
  AfterViewInit, ChangeDetectorRef, Component,
  EventEmitter, inject, Input, OnDestroy, OnInit, Output
} from '@angular/core';
import {
  AbstractControl, FormArray, FormBuilder,
  FormControl, FormGroup, FormsModule, ReactiveFormsModule
} from '@angular/forms';
import { filter, Subject, takeUntil } from 'rxjs';
import { ActivatedRoute, NavigationEnd, Router } from '@angular/router';
import { DomSanitizer, SafeHtml, SafeResourceUrl } from '@angular/platform-browser';

import { ApiService } from '../../../dependencies/services/api.service';
import { ToastrService } from '../../../dependencies/services/toastr.service';
import { URLS } from '../../../dependencies/config/api.config';
import { MarkdownRendererService } from '../../../dependencies/services/markdownrender.service';
import { DocumentViewerService } from '../../../dependencies/services/Documentviewer.service';
import { ExtractFormService } from '../../../dependencies/services/extractform.service';
import { PlaygroundStateService } from '../../../dependencies/services/Playgroundstate.service';
import { DocumentPage, UploadedFile } from '../../../../models/playground.model';
@Component({
  selector: 'app-playground',
  standalone: true,
  imports: [CommonModule, FormsModule, ReactiveFormsModule],
  templateUrl: './playground.html',
  styleUrl: './playground.scss',
})
export class Playground implements OnInit, AfterViewInit, OnDestroy {
  private readonly api = inject(ApiService);
  private readonly cdr = inject(ChangeDetectorRef);
  private readonly fb = inject(FormBuilder);
  private readonly route = inject(ActivatedRoute);
  private readonly router = inject(Router);
  private readonly sanitizer = inject(DomSanitizer);
  private readonly toastr = inject(ToastrService);
  readonly mdSvc = inject(MarkdownRendererService);
  readonly docSvc = inject(DocumentViewerService);
  readonly formSvc = inject(ExtractFormService);
  readonly stateSvc = inject(PlaygroundStateService);

  @Output() fileUploaded = new EventEmitter<UploadedFile>();
  @Input() mode!: string;
  schemaForm: FormGroup;
  extractForm: FormGroup;

  private destroy$ = new Subject<void>();

  Schemas: any[] = [];
  selectedFile: File | null = null;
  isDragging = false;
  fileUpload = false;
  activeTab: 'json' | 'markdown' | 'formalReport' = 'markdown';
  isLoading = false;
  apiResponse: any = null;
  filteredApiResponse: any = null;
  showRenderedMarkdown = true;
  safeMarkdownContent: SafeHtml = '';
  safeFormalReportContent: SafeHtml = '';

  // Document viewer
  documentPages: DocumentPage[] = [];
  selectedPreviewPage: DocumentPage | null = null;
  isExtractingPages = false;
  showThumbnails = true;
  documentUrl: any = null;
  documentFileName = '';
  isDocumentLoading = false;
  documentError = '';
  documentType: 'pdf' | 'image' | 'none' = 'none';

  // Image / PDF
  imageZoom = 100;
  imageRotation = 0;
  isFullscreen = false;
  pdfViewerKey = 0;
  previewUrl: string | null = null;
  safePreviewUrl!: SafeResourceUrl;
  isPdf = false;
  showFullPreview = false;
  selectedPdfFile!: File;

  // Extraction
  extractedData: any = {};
  isExtracting = false;
  extractionProgress = '';
  showExtractionResults = false;
  showJsonOnClick = false;
  showRawData = false;
  formattedPayload: any = null;

  // Route
  id = '';
  isFromUsage = false;
  fileDetails: any = null;

  private get stateKey(): string {
    return this.mode === 'demo' ? 'forge_playground_demo_state' : 'forge_playground_state';
  }

  constructor() {
    this.schemaForm = this.fb.group({ page_range: [''], model: ['standard'] });
    this.extractForm = this.formSvc.buildExtractForm();
    this.formSvc.addField(this.fields, '');
  }

  ngOnInit(): void {
    this.schemaForm.patchValue({ model: 'standard' });
    this.restoreState();
    this.watchRoute();
    if (this.mode !== 'demo') this.loadSchemas();
    this.setupFullscreenListener();
    this.watchRouterForStateCleanup();
    this.formSvc.ensureControls(this.fields);
    this.extractForm.valueChanges
      .pipe(takeUntil(this.destroy$))
      .subscribe(() => this.refreshPayload());
  }

  ngAfterViewInit(): void { this.initTabs(); this.cdr.detectChanges(); }

  ngOnDestroy(): void {
    if (this.safePreviewUrl) URL.revokeObjectURL(this.safePreviewUrl as any);
    this.destroy$.next();
    this.destroy$.complete();
    this.stateSvc.clearState(this.stateKey);
  }

  get fields(): FormArray { return this.extractForm.get('fields') as FormArray; }

  get markdownContent(): string {
    if (!this.apiResponse) return '# No data available\n\nPlease parse a document first.';
    const direct = this.apiResponse.markdown
      ?? this.apiResponse.parsed_data?.markdown
      ?? this.apiResponse.data?.markdown;
    if (direct) {
      this.safeMarkdownContent = this.mdSvc.toSafeHtml(direct);
      return direct;
    }
    const result = this.mdSvc.buildFromResponse(
      this.apiResponse, this.filteredApiResponse,
      this.documentFileName, this.selectedFile?.name
    );
    this.safeMarkdownContent = result.safe;
    return result.raw;
  }

  get formalReportContent(): SafeHtml {
    if (!this.apiResponse) {
      return this.sanitizer.bypassSecurityTrustHtml(
        '<div class="formal-report"><div class="no-data">No data available.</div></div>'
      );
    }
    const html = this.stateSvc.generateFormalReport(
      this.apiResponse, this.filteredApiResponse,
      this.documentFileName, this.selectedFile?.name
    );
    this.safeFormalReportContent = this.sanitizer.bypassSecurityTrustHtml(html);
    return this.safeFormalReportContent;
  }

  private loadSchemas(): void {
    this.api.get(URLS.schema).pipe(takeUntil(this.destroy$)).subscribe((res: any) => {
      if (res?.success) this.Schemas = res.schemas ?? [];
    });
  }

  loadParsedFile(id: string): void {
    this.isLoading = true;
    this.api.get(`${URLS.getFile}/file-details/${id}`).pipe(takeUntil(this.destroy$)).subscribe({
      next: res => {
        this.isLoading = false;
        res?.success ? (this.fileDetails = res.files ?? res, this.applyFileState(this.fileDetails), this.persistState())
          : this.setError('Failed to load file details');
      },
      error: () => { this.isLoading = false; this.setError('Error loading file details'); }
    });
  }

  parseDocument(): void {
    if (!this.selectedFile) return;
    this.isLoading = true;
    const fd = new FormData();
    fd.append('file', this.selectedFile);
    fd.append('page_range', this.schemaForm.get('page_range')?.value ?? '');
    fd.append('model', this.mode === 'demo' ? 'standard' : (this.schemaForm.get('model')?.value ?? 'standard'));

    this.api.post(URLS.fileUpload, fd).subscribe({
      next: (res: any) => {
        this.isLoading = false;
        if (res.success) {
          this.apiResponse = res;
          this.filteredApiResponse = res.json_output ?? res;
          this.fileUpload = true;
          this.activeTab = 'markdown';
          this.isFromUsage = false;
          this.setupViewer(res);
          void this.markdownContent;
          void this.formalReportContent;
          this.persistState();
        } else {
          this.apiResponse = { error: true, message: res.message ?? 'Failed to parse document' };
          this.filteredApiResponse = this.apiResponse;
          this.fileUpload = true;
          this.persistState();
        }
        this.cdr.detectChanges();
      },
      error: () => {
        this.isLoading = false;
        this.apiResponse = { error: true, message: 'Network error: Failed to connect to API' };
        this.filteredApiResponse = this.apiResponse;
        this.fileUpload = true;
        this.persistState();
        this.cdr.detectChanges();
      }
    });
  }


  onExtractSubmit(): void {
    if (!this.extractForm.valid) {
      this.toastr.error('Please fill all required fields before applying the schema.');
      this.formSvc.markAllTouched(this.fields);
      return;
    }
    this.isExtracting = true;
    this.extractionProgress = 'Extracting from entire document...';
    this.showExtractionResults = false;
    this.formattedPayload = this.formSvc.formatPayload(this.extractForm.value);
    setTimeout(() => { this.extractionProgress = 'Processing extracted data...'; }, 1000);

    this.api.post(URLS.extract, { ...this.formattedPayload, markdown_content: this.markdownContent }).subscribe({
      next: res => setTimeout(() => {
        this.isExtracting = false;
        this.showExtractionResults = true;
        this.extractedData = res.extracted_data ?? res;
        this.toastr.success('Data extracted successfully!');
        this.cdr.detectChanges();
      }, 1500),
      error: err => setTimeout(() => {
        this.isExtracting = false;
        this.toastr.error('Extraction failed: ' + (err.message ?? 'Unknown error'));
        this.cdr.detectChanges();
      }, 1500)
    });
  }

  private persistState(): void {
    if (!this.fileUpload || !this.apiResponse) return;
    this.stateSvc.saveState(this.stateKey,
      this.stateSvc.buildState(
        this.apiResponse, this.filteredApiResponse, this.fileDetails, this.isFromUsage,
        this.documentFileName, this.activeTab, this.documentType,
        this.showThumbnails, this.imageZoom, this.imageRotation
      )
    );
  }

  private restoreState(): void {
    const s = this.stateSvc.loadState(this.stateKey);
    if (!s) return;
    Object.assign(this, {
      fileUpload: true, apiResponse: s.apiResponse, filteredApiResponse: s.filteredApiResponse,
      fileDetails: s.fileDetails, isFromUsage: s.isFromUsage, documentFileName: s.documentFileName,
      activeTab: 'markdown', documentType: s.documentType ?? 'none',
      showThumbnails: s.showThumbnails ?? true, imageZoom: s.imageZoom ?? 100, imageRotation: s.imageRotation ?? 0
    });
    this.restoreViewer();
    void this.markdownContent; void this.formalReportContent;
    this.cdr.detectChanges();
  }

  setActiveTab(tab: 'json' | 'markdown' | 'formalReport'): void { this.activeTab = tab; this.persistState(); }

  private setupViewer(fileData: any): void {
    const meta = fileData.metadata ?? fileData;
    if (!meta?.file_path) {
      this.documentType = 'none'; this.documentUrl = null;
      this.documentError = 'No document file available for preview'; return;
    }
    this.isDocumentLoading = true; this.documentPages = []; this.selectedPreviewPage = null; this.documentError = '';
    const ext = this.docSvc.getFileExtension(meta.file_path);
    const fullUrl = this.docSvc.buildFullUrl(meta.file_path);
    this.documentFileName = meta.filename ?? meta.original_filename ?? 'document';

    if (ext === 'pdf') {
      this.documentType = 'pdf'; this.documentUrl = null;
    } else if (this.docSvc.isSupportedImage(ext)) {
      this.documentType = 'image';
      this.documentUrl = this.docSvc.buildSafeResourceUrl(fullUrl);
      this.imageZoom = 100; this.imageRotation = 0;
    } else {
      this.documentType = 'none'; this.documentError = `Unsupported file type: ${ext}`; return;
    }

    const isSame = this.selectedFile &&
      (this.selectedFile.name === this.documentFileName || this.documentFileName.includes(this.selectedFile.name));
    isSame && this.selectedFile
      ? this.runPageExtraction(this.selectedFile)
      : this.documentPages.length
        ? (this.selectedPreviewPage = this.documentPages[0], this.isDocumentLoading = false)
        : this.runRemotePageExtraction(fullUrl);
    this.cdr.detectChanges();
  }

  private restoreViewer(): void {
    if (this.documentPages.length) {
      this.isDocumentLoading = false; this.selectedPreviewPage = this.documentPages[0];
      this.cdr.detectChanges(); return;
    }
    const src = this.fileDetails ?? this.apiResponse;
    if (src) { this.setupViewer(src); return; }
    this.documentType = 'none'; this.documentUrl = null; this.isDocumentLoading = false;
  }

  private async runPageExtraction(file: File): Promise<void> {
    this.isExtractingPages = true; this.documentPages = []; this.cdr.detectChanges();
    try {
      this.documentPages = await this.docSvc.extractPages(file);
      if (this.documentPages.length) this.selectedPreviewPage = this.documentPages[0];
    } catch (e) { console.error('Page extraction failed:', e); }
    finally { this.isExtractingPages = false; this.isDocumentLoading = false; this.cdr.detectChanges(); }
  }

  private async runRemotePageExtraction(url: string): Promise<void> {
    this.isExtractingPages = true; this.isDocumentLoading = true;
    try {
      this.documentPages = await this.docSvc.fetchAndExtractPages(url);
      if (this.documentPages.length) this.selectedPreviewPage = this.documentPages[0];
    } catch { this.documentError = 'Unable to load document preview. Please try re-uploading the file.'; }
    finally { this.isExtractingPages = false; this.isDocumentLoading = false; this.cdr.detectChanges(); }
  }

  async extractDocumentPages(file: File): Promise<void> { await this.runPageExtraction(file); }

  onDocumentError(): void {
    this.isDocumentLoading = false;
    this.documentError = 'Failed to load document. The file may be unavailable or corrupted.';
    this.cdr.detectChanges();
  }

  toggleThumbnails(): void { this.showThumbnails = !this.showThumbnails; }
  selectPagePreview(p: DocumentPage): void { this.selectedPreviewPage = p; }
  prevPage(): void {
    const i = this.documentPages.findIndex(p => p.pageNumber === this.selectedPreviewPage?.pageNumber);
    if (i > 0) this.selectedPreviewPage = this.documentPages[i - 1];
  }
  nextPage(): void {
    const i = this.documentPages.findIndex(p => p.pageNumber === this.selectedPreviewPage?.pageNumber);
    if (i < this.documentPages.length - 1) this.selectedPreviewPage = this.documentPages[i + 1];
  }

  zoomIn(): void { if (this.imageZoom < 200) { this.imageZoom += 10; this.persistState(); } }
  zoomOut(): void { if (this.imageZoom > 50) { this.imageZoom -= 10; this.persistState(); } }

  triggerFileInput(): void { (document.getElementById('fileInput') as HTMLInputElement)?.click(); }

  onFileSelected(e: any): void {
    const f = e.target.files[0];
    if (f && this.docSvc.isValidFileType(f)) this.handleFile(f);
    e.target.value = '';
  }
  onDragOver(e: DragEvent): void { e.preventDefault(); this.isDragging = true; }
  onDragLeave(e: DragEvent): void { e.preventDefault(); this.isDragging = false; }
  onDrop(e: DragEvent): void {
    e.preventDefault(); this.isDragging = false;
    const f = e.dataTransfer?.files[0];
    if (f && this.docSvc.isValidFileType(f)) this.handleFile(f);
  }

  private handleFile(file: File): void {
    this.selectedFile = file; this.isFromUsage = false;
    this.stateSvc.clearState(this.stateKey);
    this.documentPages = []; this.selectedPreviewPage = null;
    this.generatePreview(file);
    this.runPageExtraction(file);
  }

  generatePreview(file: File): void {
    this.isPdf = file.type === 'application/pdf';
    if (this.isPdf) { this.selectedPdfFile = file; this.safePreviewUrl = this.docSvc.buildPdfPreviewUrl(file); }
    else { const r = new FileReader(); r.onload = () => { this.previewUrl = r.result as string; }; r.readAsDataURL(file); }
  }

  openFullPreview(): void {
    if (this.isPdf && this.selectedPdfFile) { this.safePreviewUrl = this.docSvc.buildPdfPreviewUrl(this.selectedPdfFile); this.pdfViewerKey++; }
    this.showFullPreview = true;
  }
  closeFullPreview(): void { this.showFullPreview = false; }

  changeDocument(): void {
    this.resetState(); this.previewUrl = null; this.isPdf = false;
    this.documentPages = []; this.selectedPreviewPage = null;
    const fi = document.getElementById('fileInput') as HTMLInputElement;
    if (fi) fi.value = '';
  }
  resetFileState(): void {
    this.isFromUsage ? this.router.navigate(['/main/playground']) : this.resetState();
    this.stateSvc.clearState(this.stateKey);
  }

  copyToClipboard(type: 'json' | 'markdown'): void {
    const c = type === 'json' ? JSON.stringify(this.filteredApiResponse, null, 2) : this.markdownContent;
    navigator.clipboard.writeText(c)
      .then(() => this.toastr.success(`${type} copied to clipboard`))
      .catch(err => this.toastr.error(err));
  }

  downloadFile(): void {
    if (this.documentUrl) {
      const a = Object.assign(document.createElement('a'), { href: this.documentUrl.toString(), download: this.documentFileName, target: '_blank' });
      document.body.appendChild(a); a.click(); document.body.removeChild(a);

    } else if (this.selectedFile) {
      const url = URL.createObjectURL(this.selectedFile);
      const a = Object.assign(document.createElement('a'), { href: url, download: this.selectedFile.name });
      document.body.appendChild(a); a.click(); document.body.removeChild(a);
      URL.revokeObjectURL(url);

    } else {
      const src = this.fileDetails ?? this.apiResponse;
      const meta = src?.metadata ?? src;
      if (meta?.file_path) {
        const remoteUrl = this.docSvc.buildFullUrl(meta.file_path);
        const a = Object.assign(document.createElement('a'), { href: remoteUrl, download: this.documentFileName || meta.filename || meta.original_filename || 'document', target: '_blank' });
        document.body.appendChild(a); a.click(); document.body.removeChild(a);
      }
    }
  }

  downloadJsonFile(): void { if (!this.apiResponse) return; this.docSvc.triggerDownload(JSON.stringify(this.filteredApiResponse, null, 2), 'application/json', this.docSvc.buildFileName(this.selectedFile, 'json')); }
  downloadMarkdownFile(): void { this.docSvc.triggerDownload(this.markdownContent, 'text/markdown', this.docSvc.buildFileName(this.selectedFile, 'md')); }

  downloadFormalReport(): void {
    const html = this.stateSvc.generateFormalReport(this.apiResponse, this.filteredApiResponse, this.documentFileName, this.selectedFile?.name);
    this.docSvc.triggerDownload(
      `<!DOCTYPE html><html><head><title>Formal Report</title><style>${document.querySelector('style')?.textContent ?? ''}</style></head><body>${html}</body></html>`,
      'text/html', this.docSvc.buildFileName(this.selectedFile, 'html')
    );
  }

  copyExtractedResults(): void {
    if (!this.extractedData) return;
    navigator.clipboard.writeText(JSON.stringify(this.extractedData, null, 2))
      .then(() => this.toastr.success('Extracted results copied to clipboard'))
      .catch(err => this.toastr.error('Failed to copy: ' + err));
  }

  downloadExtractedJson(): void {
    if (!this.extractedData) return;
    this.docSvc.triggerDownload(JSON.stringify(this.extractedData, null, 2), 'application/json', this.docSvc.buildFileName(this.selectedFile, '_extracted.json'));
  }

  getFieldTypeOptions(): string[] { return this.formSvc.getFieldTypeOptions(); }
  getPrimitiveTypeOptions(): string[] { return this.formSvc.getPrimitiveTypeOptions(); }
  getValueTypeOptions(f: AbstractControl): string[] { return this.formSvc.getValueTypeOptions(f); }
  isArrayType(f: AbstractControl): boolean { return this.formSvc.isArrayType(f); }
  isArrayObjectType(f: AbstractControl): boolean { return this.formSvc.isArrayObjectType(f); }
  getFieldGroup(f: AbstractControl): FormGroup { return this.formSvc.asGroup(f); }
  getFormGroup(f: AbstractControl): FormGroup { return this.formSvc.asGroup(f); }
  getFormControl(g: FormGroup, n: string): FormControl { return this.formSvc.asControl(g, n); }
  getSubFields(g: FormGroup): FormArray { return this.formSvc.getSubFields(g); }
  getItemsFormGroup(g: FormGroup): FormGroup | null { return this.formSvc.getItemsFormGroup(g); }
  getArrayItemProperties(g: FormGroup): FormArray | null { return this.formSvc.getArrayItemProperties(g); }

  addField(n?: string, t?: string, d?: string, r?: boolean): void { this.formSvc.addField(this.fields, this.mode, n, t, d, r); }
  removeField(i: number): void { this.formSvc.removeField(this.fields, i); }
  addSubField(g: FormGroup): void { this.formSvc.addSubField(g); }
  removeSubField(g: FormGroup, i: number): void { this.formSvc.removeSubField(g, i); }
  addArrayItemProperty(g: FormGroup): void { this.formSvc.addArrayItemProperty(g); }
  removeArrayItemProperty(g: FormGroup, i: number): void { this.formSvc.removeArrayItemProperty(g, i); }

  onTypeChange(idx: number): void { this.formSvc.onTypeChange(this.fields, idx); }
  onArrayItemTypeChange(idx: number): void { this.formSvc.onArrayItemTypeChange(this.fields, idx); this.refreshPayload(); }
  ensureFormControlsExist(): void { this.formSvc.ensureControls(this.fields); }
  refreshPayload(): void { this.formattedPayload = this.formSvc.formatPayload(this.extractForm.value); this.cdr.detectChanges(); }

  toggleMarkdownView(): void { this.showRenderedMarkdown = !this.showRenderedMarkdown; }
  toggleJsonView(): void { this.showJsonOnClick = !this.showJsonOnClick; }
  showOrHideJson(): void { this.showRawData = !this.showRawData; }

  formatFileSize(b: number): string { return this.docSvc.formatFileSize(b); }
  getDataSections(): any[] { return this.formSvc.getDataSections(this.extractedData); }
  parseData(d: any): any[] { return this.formSvc.parseData(d); }

  onDocTypeChange(id: any): void {
    if (!id) return;
    if (id === 'others') { while (this.fields.length) this.fields.removeAt(0); this.addField(); this.refreshPayload(); this.cdr.detectChanges(); return; }
    const schema = this.Schemas.find(s => s.doc_type_id == id);
    if (!schema?.extraction_schema) { this.toastr.warning('No PreDefined Schema found for this document type'); return; }
    const parsed = this.formSvc.parseSchemaFields(schema.extraction_schema);
    if (!parsed?.length) { this.toastr.warning('No schema fields found for this document type'); return; }
    this.formSvc.loadFieldsIntoArray(this.fields, this.mode, parsed);
    this.toastr.success('Schema loaded successfully');
    this.refreshPayload(); this.cdr.detectChanges();
  }

  // ── Private helpers ───────────────────────────────────────────────────────────

  private applyFileState(d: any): void {
    this.fileUpload = true;
    this.apiResponse = d;
    this.filteredApiResponse = d.json ?? d.parsed_data?.json ?? d.json_output ?? d;
    this.setupViewer(d);
    this.cdr.detectChanges();
  }

  private setError(msg: string): void {
    this.apiResponse = { error: true, message: msg };
    this.filteredApiResponse = this.apiResponse;
    this.fileUpload = true;
    this.cdr.detectChanges();
  }

  private resetState(): void {
    Object.assign(this, {
      fileUpload: false, selectedFile: null, apiResponse: null, filteredApiResponse: null,
      fileDetails: null, isFromUsage: false, documentUrl: null, documentFileName: '',
      isDocumentLoading: false, documentError: '', documentType: 'none',
      imageZoom: 100, imageRotation: 0, isFullscreen: false,
      safeMarkdownContent: '', safeFormalReportContent: '',
      activeTab: 'json', showExtractionResults: false, extractedData: {}
    });
    this.schemaForm.reset({ model: 'standard', page_range: '' });
    this.stateSvc.clearState(this.stateKey);
  }

  private watchRoute(): void {
    this.route.paramMap.pipe(takeUntil(this.destroy$)).subscribe(p => {
      this.id = p.get('id') ?? '';
      this.isFromUsage = !!this.id;
      if (this.id) { this.stateSvc.clearState(this.stateKey); this.loadParsedFile(this.id); }
    });
  }

  private watchRouterForStateCleanup(): void {
    this.router.events.pipe(filter(e => e instanceof NavigationEnd), takeUntil(this.destroy$))
      .subscribe((e: NavigationEnd) => { if (!e.url.includes('/main/playground')) this.stateSvc.clearState(this.stateKey); });
  }

  private setupFullscreenListener(): void {
    document.addEventListener('fullscreenchange', () => { this.isFullscreen = !!document.fullscreenElement; this.cdr.detectChanges(); });
  }

  private initTabs(): void {
    setTimeout(() => {
      document.querySelectorAll('#nav-tab button[data-bs-toggle="tab"]').forEach(el => {
        const tab = new (window as any).bootstrap.Tab(el);
        el.addEventListener('click', (e: Event) => { e.preventDefault(); tab.show(); });
      });
    }, 100);
  }
}