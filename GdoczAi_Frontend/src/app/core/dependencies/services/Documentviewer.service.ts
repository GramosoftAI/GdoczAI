import { inject, Injectable } from '@angular/core';
import { DomSanitizer, SafeResourceUrl } from '@angular/platform-browser';
import * as pdfjsLib from 'pdfjs-dist/legacy/build/pdf.mjs';
import { environment } from '../../../../environments/environment';
import { DocumentPage } from '../../../models/playground.model';

(pdfjsLib as any).GlobalWorkerOptions.workerSrc =
  'https://unpkg.com/pdfjs-dist@5.5.207/legacy/build/pdf.worker.min.mjs';

@Injectable({ providedIn: 'root' })
export class DocumentViewerService {
  private readonly sanitizer      = inject(DomSanitizer);
  readonly baseDocumentUrl        = `${environment.apiUrl}stored_documents`;

  // ── URL helpers ──────────────────────────────────────────────────────────────

  buildFullUrl(filePath: string): string {
    return `${this.baseDocumentUrl}${filePath}`;
  }

  getFileExtension(path: string): string {
    return path.split('.').pop()?.toLowerCase() ?? '';
  }

  isSupportedImage(ext: string): boolean {
    return ['jpg', 'jpeg', 'png', 'gif', 'webp', 'bmp'].includes(ext);
  }

  isValidFileType(file: File): boolean {
    return ['image/jpeg', 'image/jpg', 'image/png', 'image/webp', 'image/bmp', 'application/pdf']
      .includes(file.type);
  }

  // ── Safe URL builders ────────────────────────────────────────────────────────

  buildSafeResourceUrl(url: string): SafeResourceUrl {
    return this.sanitizer.bypassSecurityTrustResourceUrl(url);
  }

  buildPdfPreviewUrl(file: File): SafeResourceUrl {
    return this.sanitizer.bypassSecurityTrustResourceUrl(
      URL.createObjectURL(file) + '#toolbar=0&zoom=page-width'
    );
  }

  // ── Page extraction ──────────────────────────────────────────────────────────

  async extractPages(file: File): Promise<DocumentPage[]> {
    if (file.type === 'application/pdf') {
      return this.extractPdfPages(file);
    }
    const dataUrl = await this.fileToDataUrl(file);
    return [{ pageNumber: 1, thumbnail: dataUrl }];
  }

  async fetchAndExtractPages(url: string): Promise<DocumentPage[]> {
    const response = await fetch(url, { mode: 'cors', credentials: 'omit' });
    if (!response.ok) throw new Error(`Failed to fetch document: ${response.status}`);
    const blob = await response.blob();
    const file = new File([blob], url.split('/').pop() ?? 'document', { type: blob.type });
    return this.extractPages(file);
  }

  private async extractPdfPages(file: File): Promise<DocumentPage[]> {
    const pages: DocumentPage[] = [];
    const ab  = await file.arrayBuffer();
    const pdf = await (pdfjsLib as any).getDocument({ data: ab }).promise;

    for (let i = 1; i <= pdf.numPages; i++) {
      const page     = await pdf.getPage(i);
      const viewport = page.getViewport({ scale: 1.5 });
      const canvas   = document.createElement('canvas');
      const ctx      = canvas.getContext('2d')!;
      canvas.width   = viewport.width;
      canvas.height  = viewport.height;
      await page.render({ canvasContext: ctx, viewport }).promise;
      pages.push({ pageNumber: i, thumbnail: canvas.toDataURL('image/jpeg', 0.95) });
    }

    return pages;
  }

  fileToDataUrl(file: File): Promise<string> {
    return new Promise((res, rej) => {
      const reader = new FileReader();
      reader.onload  = () => res(reader.result as string);
      reader.onerror = rej;
      reader.readAsDataURL(file);
    });
  }

  // ── Download trigger ─────────────────────────────────────────────────────────

  triggerDownload(content: string, mimeType: string, fileName: string): void {
    const url = URL.createObjectURL(new Blob([content], { type: mimeType }));
    const a   = document.createElement('a');
    a.href = url; a.download = fileName;
    document.body.appendChild(a); a.click(); document.body.removeChild(a);
    URL.revokeObjectURL(url);
  }

  buildFileName(selectedFile: File | null, ext: string): string {
    const base = selectedFile
      ? selectedFile.name.replace(/\.[^/.]+$/, '')
      : 'document_analysis';
    return `${base}.${ext}`;
  }

  formatFileSize(bytes: number): string {
    if (!bytes) return '0 Bytes';
    const k = 1024, sizes = ['Bytes', 'KB', 'MB', 'GB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return `${parseFloat((bytes / Math.pow(k, i)).toFixed(2))} ${sizes[i]}`;
  }
}