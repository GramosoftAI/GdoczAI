// ── Public interfaces ────────────────────────────────────────────────────────

export interface UploadedFile {
  id: string; name: string; size: number; type: string; uploadDate: Date;
}

export interface DocumentPage {
  pageNumber: number;
  thumbnail: string;
}

export interface PlaygroundState {
  hasData: boolean;
  apiResponse: any;
  filteredApiResponse: any;
  fileDetails: any;
  isFromUsage: boolean;
  documentFileName: string;
  timestamp: number;
  activeTab: 'json' | 'markdown' | 'formalReport';
  documentType: 'pdf' | 'image' | 'none';
  showThumbnails: boolean;
  imageZoom: number;
  imageRotation: number;
}

export interface HighlightCard {
  icon: string; title: string; value: string; type: string; tooltip?: string;
}

export interface DetailItem {
  label: string; value: any; isAmount?: boolean;
}

export interface ExtractedKeyValue {
  key: string; value: any; path: string;
}

export interface ServiceTable {
  index: number; rows: any[]; totalServices: number;
}

export interface ServiceInfo {
  services: any[]; tables: ServiceTable[];
}

export interface InvoiceInfo {
  invoiceNumber?: string; invoiceDate?: string; totalAmount?: string;
  amountInWords?: string; financialItems?: ExtractedKeyValue[];
}

export interface FieldConfig {
  field_name: string; type: string; description: string; required: boolean;
  fields?: SubFieldConfig[];
}

export interface SubFieldConfig {
  field_name: string; type: string; description: string; required: boolean;
}