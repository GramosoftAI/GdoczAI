import { inject, Injectable } from '@angular/core';
import { DomSanitizer } from '@angular/platform-browser';
import {
  DetailItem, ExtractedKeyValue, HighlightCard,
  InvoiceInfo, PlaygroundState, ServiceInfo, ServiceTable
} from '../../../models/playground.model';
import { MarkdownRendererService } from './markdownrender.service';


@Injectable({ providedIn: 'root' })
export class PlaygroundStateService {
  private readonly sanitizer   = inject(DomSanitizer);
  private readonly markdownSvc = inject(MarkdownRendererService);

  // ── Local-storage persistence ────────────────────────────────────────────────

  saveState(key: string, state: PlaygroundState): void {
    localStorage.setItem(key, JSON.stringify(state));
  }

  loadState(key: string): PlaygroundState | null {
    const raw = localStorage.getItem(key);
    if (!raw) return null;
    try {
      const state: PlaygroundState = JSON.parse(raw);
      const valid = state.timestamp && (Date.now() - state.timestamp) < 2 * 60 * 60 * 1000;
      return valid && state.hasData ? state : null;
    } catch { return null; }
  }

  clearState(key: string): void {
    localStorage.removeItem(key);
  }

  buildState(
    apiResponse: any, filteredApiResponse: any, fileDetails: any,
    isFromUsage: boolean, documentFileName: string,
    activeTab: 'json' | 'markdown' | 'formalReport',
    documentType: 'pdf' | 'image' | 'none',
    showThumbnails: boolean, imageZoom: number, imageRotation: number
  ): PlaygroundState {
    return {
      hasData: true, apiResponse, filteredApiResponse, fileDetails,
      isFromUsage, documentFileName, activeTab, documentType,
      showThumbnails, imageZoom, imageRotation, timestamp: Date.now()
    };
  }

  // ── Formal report ─────────────────────────────────────────────────────────────

  generateFormalReport(
    data: any, filteredData: any,
    documentFileName: string, selectedFileName?: string
  ): string {
    const ts       = new Date().toLocaleString();
    const fileName = documentFileName || selectedFileName || 'Unknown';
    return `
      <div class="formal-report invoice-report">
        <header class="report-header">
          <div class="header-content">
            <div class="company-info"><p class="company-tagline">Document Analysis Report</p></div>
            <div class="report-meta">
              <div class="meta-item"><span class="meta-label">Report Generated:</span><span class="meta-value">${ts}</span></div>
              <div class="meta-item"><span class="meta-label">File:</span><span class="meta-value">${fileName}</span></div>
            </div>
          </div>
          <div class="header-decoration"><div class="decoration-line"></div></div>
        </header>
        <main class="report-content">${this.generateSections(filteredData ?? data)}</main>
      </div>`;
  }

  private generateSections(data: any): string {
    const invoice  = this.extractInvoiceData(data);
    const merchant = this.extractPartyInfo(data, 'merchant');
    const customer = this.extractPartyInfo(data, 'customer');
    const vehicle  = this.extractVehicleInfo(data);
    const services = this.extractServiceInfo(data);

    let s = this.overviewSection(invoice);
    if (Object.keys(merchant).length) s += this.partySection('merchant', merchant);
    if (Object.keys(customer).length) s += this.partySection('customer', customer);
    if (Object.keys(vehicle).length)  s += this.vehicleSection(vehicle);
    if (services.services.length)     s += this.servicesSection(services);
    if (invoice.financialItems?.length) s += this.financialSection(invoice);
    return s;
  }

  // ── Section builders ─────────────────────────────────────────────────────────

  private overviewSection(inv: InvoiceInfo): string {
    const cards: HighlightCard[] = [];
    if (inv.invoiceNumber) cards.push({ icon: '📄', title: 'Invoice/Document Number', value: inv.invoiceNumber, type: 'primary' });
    if (inv.invoiceDate) {
      let d = inv.invoiceDate;
      try { d = new Date(inv.invoiceDate).toLocaleDateString('en-IN', { day: '2-digit', month: '2-digit', year: 'numeric' }); } catch { }
      cards.push({ icon: '📅', title: 'Document Date', value: d, type: 'info' });
    }
    if (inv.totalAmount)   cards.push({ icon: '💰', title: 'Total Amount', value: this.fmt(inv.totalAmount), type: 'success' });
    if (inv.amountInWords) cards.push({ icon: '📝', title: 'Amount in Words', value: this.truncate(inv.amountInWords, 60), type: 'secondary' });

    return `
      <section class="report-section invoice-overview">
        <div class="section-header"><h2 class="section-title"><span class="title-icon">📋</span>Document Overview</h2><div class="section-decoration"></div></div>
        <div class="section-content">
          ${cards.length ? `<div class="highlights-grid">${cards.map((c, i) => `
            <div class="highlight-card ${c.type}" style="animation-delay:${i * 0.1}s">
              <div class="highlight-content" style="display:flex;align-items:baseline;justify-content:space-between">
                <div style="display:flex;align-items:baseline;"><div class="highlight-icon">${c.icon}</div><div class="highlight-title">${c.title}</div></div>
                <div class="highlight-value">${c.value}</div>
              </div>
            </div>`).join('')}</div>` : '<p class="text-muted">No document overview information extracted</p>'}
          ${this.metricsSection(inv)}
        </div>
      </section>`;
  }

  private metricsSection(inv: InvoiceInfo): string {
    if (!inv.financialItems?.length) return '';
    const grouped: Record<string, number> = {};
    inv.financialItems.forEach(item => {
      const v = parseFloat(item.value) || 0;
      if (v > 0) {
        const k = this.markdownSvc.formatKey(item.key);
        grouped[k] = (grouped[k] || 0) + v;
      }
    });
    const metrics = Object.entries(grouped).map(([label, value]) => {
      const l = label.toLowerCase();
      const icon  = l.includes('labour') || l.includes('service') ? '👨‍🔧'
                  : l.includes('part')   ? '⚙️'
                  : l.includes('tax')    || l.includes('gst') ? '🧾'
                  : l.includes('sublet') ? '🛠️' : '💰';
      const trend = l.includes('labour') || l.includes('service') ? 'labour'
                  : l.includes('part')   ? 'parts'
                  : l.includes('tax')    || l.includes('gst') ? 'tax'
                  : l.includes('sublet') ? 'sublet' : 'financial';
      return { label, value: this.fmt(value.toString()), icon, trend };
    });
    return `
      <div class="metrics-section">
        <h3 class="metrics-title">Financial Breakdown</h3>
        ${metrics.length ? `<div class="metrics-grid">${metrics.map(m => `
          <div class="metric-card ${m.trend}">
            <div class="metric-content" style="display:flex;align-items:baseline;justify-content:space-between">
              <div style="display:flex;align-items:baseline;"><div class="metric-icon">${m.icon}</div><div class="metric-label">${m.label}</div></div>
              <div class="metric-value">${m.value}</div>
            </div>
          </div>`).join('')}</div>` : ''}
      </div>`;
  }

  private partySection(type: 'merchant' | 'customer', data: any): string {
    const title = type === 'merchant' ? 'Merchant / Service Center' : 'Customer Details';
    const badge = type === 'merchant' ? 'Dealer' : 'Client';
    const icon  = type === 'merchant' ? '🏢' : '👤';
    const items: DetailItem[] = Object.keys(data)
      .filter(k => data[k] && !Array.isArray(data[k]) && typeof data[k] !== 'object')
      .map(k => ({ label: this.markdownSvc.formatKey(k), value: data[k] }));
    return `
      <section class="report-section party-information">
        <div class="section-header"><h2 class="section-title"><span class="title-icon">${icon}</span>${title}</h2><div class="section-decoration"></div></div>
        <div class="section-content">
          <div class="party-card ${type}">
            <div class="party-header"><h3 class="party-title">${title}</h3><span class="party-badge" style="font-size:18px;"><b>${badge}</b></span></div>
            <div class="party-content">
              ${items.length ? `<div class="party-details">${items.map(d => `
                <div class="detail-row"><span class="detail-label"><b>${d.label}:</b></span><span class="detail-value" style="margin-left:5px;">${d.value}</span></div>`).join('')}</div>`
              : '<div class="no-party-data">No information extracted</div>'}
            </div>
          </div>
        </div>
      </section>`;
  }

  private vehicleSection(data: any): string {
    const items: DetailItem[] = Object.keys(data)
      .filter(k => data[k] && !Array.isArray(data[k]) && typeof data[k] !== 'object')
      .map(k => ({ label: this.markdownSvc.formatKey(k), value: data[k] }));
    return `
      <section class="report-section vehicle-details">
        <div class="section-header"><h2 class="section-title"><span class="title-icon">🚗</span>Vehicle Information</h2><div class="section-decoration"></div></div>
        <div class="section-content">
          ${items.length ? `<div class="vehicle-details-grid">${items.map(d => `
            <div class="vehicle-detail-card"><span class="vehicle-detail-label"><b>${d.label}:</b></span><span class="vehicle-detail-value" style="margin-left:5px;">${d.value}</span></div>`).join('')}</div>`
          : '<div class="no-vehicle-data">No vehicle information extracted</div>'}
        </div>
      </section>`;
  }

  private servicesSection(serviceData: ServiceInfo): string {
    const services = serviceData.services.slice(0, 10);
    return `
      <section class="report-section service-details">
        <div class="section-header"><h2 class="section-title"><span class="title-icon">🔧</span>Service Details</h2><div class="section-decoration"></div></div>
        <div class="section-content">
          <div class="services-header"><h3>Services Performed</h3><span class="services-count">${services.length} service(s) found</span></div>
          <div class="services-grid">${services.map((svc: any, i: number) => `
            <div class="service-card">
              <div class="service-header">
                <span class="service-number">#${i + 1}</span>
                <span class="service-type">${svc['Work Order Type'] ?? svc['Work_Order_Type'] ?? svc['WorkOrderType'] ?? 'Service'}</span>
              </div>
              <div class="service-content">${this.serviceDetails(svc)}</div>
            </div>`).join('')}
          </div>
        </div>
      </section>`;
  }

  private serviceDetails(svc: any): string {
    const items = Object.keys(svc)
      .filter(k => svc[k] != null && !Array.isArray(svc[k]) && typeof svc[k] !== 'object' && String(svc[k]).trim())
      .map(k => ({ label: this.markdownSvc.formatKey(k), value: svc[k], isAmount: /amount|rate|total/i.test(k) }));
    return items.length
      ? `<div class="service-details">${items.map(d => `
          <div class="service-detail d-flex justify-content-between my-1" style="border-bottom:1px solid #e5e3e3;">
            <span class="detail-label"><b>${d.label}:</b></span>
            <span class="detail-value">${d.isAmount ? `₹ ${this.fmt(d.value)}` : d.value}</span>
          </div>`).join('')}</div>`
      : '<div class="no-service-details">No detailed service information available</div>';
  }

  private financialSection(inv: InvoiceInfo): string {
    if (!inv.financialItems?.length) return '';
    const groups: Record<string, number> = {};
    const others: any[] = [];

    inv.financialItems.forEach(item => {
      const v = parseFloat(item.value) || 0;
      if (!v) return;
      const k = item.key.toLowerCase();
      const label =
        k.includes('labour')  ? 'Labour'  :
        k.includes('part')    ? 'Parts'   :
        k.includes('sublet')  ? 'Sublet'  :
        k.includes('cgst')    ? 'CGST'    :
        k.includes('sgst')    ? 'SGST'    :
        k.includes('igst')    ? 'IGST'    :
        (k.includes('total') || k.includes('gross') || k.includes('net')) ? 'Total' : null;
      if (label) groups[label] = (groups[label] || 0) + v;
      else others.push(item);
    });

    others.forEach(item => {
      const v = parseFloat(item.value) || 0;
      if (v) groups[this.markdownSvc.formatKey(item.key)] = v;
    });

    const items = Object.entries(groups)
      .map(([label, value]) => ({ label, value, type: this.financialType(label) }))
      .sort((a, b) => a.label === 'Total' ? 1 : b.label === 'Total' ? -1 : b.value - a.value);

    return `
      <section class="report-section financial-summary">
        <div class="section-header"><h2 class="section-title"><span class="title-icon">💰</span>Financial Summary</h2><div class="section-decoration"></div></div>
        <div class="section-content">
          <div class="financial-summary-section">
            <div class="summary-header"><h3>Invoice Summary</h3><span class="summary-currency">INR</span></div>
            <div class="financial-items">
              ${items.map(i => `
                <div class="financial-item ${i.type}">
                  <span class="item-label">${i.label}</span>
                  <span class="item-value">₹ ${this.fmt(i.value.toString())}</span>
                </div>`).join('')}
            </div>
            ${inv.amountInWords ? `<div class="amount-in-words"><strong>Amount in Words:</strong> ${inv.amountInWords}</div>` : ''}
          </div>
        </div>
      </section>`;
  }

  private financialType(label: string): string {
    const l = label.toLowerCase();
    if (l.includes('labour') || l.includes('service')) return 'labour';
    if (l.includes('part'))   return 'parts';
    if (l.includes('sublet')) return 'sublet';
    if (l.includes('cgst') || l.includes('sgst') || l.includes('igst')) return 'tax';
    if (l.includes('total') || l.includes('gross') || l.includes('net')) return 'total';
    return 'other';
  }

  // ── Data extraction helpers ──────────────────────────────────────────────────

  private searchInObject(obj: any, keys: string[]): any {
    if (!obj || typeof obj !== 'object') return null;
    for (const k of Object.keys(obj)) {
      const nk = k.toLowerCase().replace(/[^a-z0-9]/g, '');
      if (keys.some(t => nk.includes(t.toLowerCase().replace(/[^a-z0-9]/g, '')))) return obj[k];
    }
    for (const k of Object.keys(obj)) {
      const v = obj[k];
      if (Array.isArray(v)) { for (const item of v) { const r = this.searchInObject(item, keys); if (r) return r; } }
      else if (v && typeof v === 'object') { const r = this.searchInObject(v, keys); if (r) return r; }
    }
    return null;
  }

  private extractKV(obj: any, include: string[] = [], exclude: string[] = []): ExtractedKeyValue[] {
    const results: ExtractedKeyValue[] = [];
    if (!obj || typeof obj !== 'object') return results;
    const traverse = (o: any, path: string[] = []) => {
      for (const k of Object.keys(o)) {
        const v   = o[k];
        const nk  = k.toLowerCase().replace(/[^a-z0-9]/g, '');
        const inc = !include.length || include.some(p => nk.includes(p.toLowerCase().replace(/[^a-z0-9]/g, '')));
        const exc = exclude.some(p => nk.includes(p.toLowerCase().replace(/[^a-z0-9]/g, '')));
        if (inc && !exc && v != null && !Array.isArray(v) && typeof v !== 'object')
          results.push({ key: k, value: v, path: [...path, k].join('.') });
        if (v && typeof v === 'object') traverse(v, [...path, k]);
      }
    };
    traverse(obj);
    return results;
  }

  private findTables(obj: any): any[] {
    const tables: any[] = [];
    const traverse = (o: any) => {
      if (!o || typeof o !== 'object') return;
      if (o.table && Array.isArray(o.table)) tables.push(o.table);
      if (Array.isArray(o) && o.length > 0 && typeof o[0] === 'object' && Object.keys(o[0]).length > 2) tables.push(o);
      if (typeof o === 'object') Object.keys(o).forEach(k => traverse(o[k]));
    };
    traverse(obj);
    return tables;
  }

  extractInvoiceData(data: any): InvoiceInfo {
    if (!data) return {};
    const inv: InvoiceInfo = {};
    const n = (keys: string[]) => this.searchInObject(data, keys);

    const num = n(['invoiceno','invoicenumber']); if (num) inv.invoiceNumber = num;
    const dt  = n(['invoicedate','date','datetime','timestamp']); if (dt) inv.invoiceDate = dt;
    const amt = n(['total','amount','grandtotal','gross','invoicevalue']); if (amt) inv.totalAmount = amt;
    const wrd = n(['inwords','amountwords','amountinwords']); if (wrd) inv.amountInWords = wrd;

    inv.financialItems = this.extractKV(data,
      ['amount','total','gross','net','labour','parts','sublet','cgst','sgst','igst','tax','charge','cost','price','value','sum'],
      ['inwords','date','time','no','number','id']
    );
    return inv;
  }

  extractPartyInfo(data: any, type: 'merchant' | 'customer'): any {
    if (!data) return {};
    const terms = type === 'merchant'
      ? ['merchant','dealer','vendor','seller','company','business','servicecenter']
      : ['customer','client','buyer','purchaser','name','address','contact'];
    const result: any = {};
    this.extractKV(data, terms, ['amount','total','date','time','invoice','tax'])
      .forEach(d => { result[d.key] = d.value; });
    return result;
  }

  extractVehicleInfo(data: any): any {
    if (!data) return {};
    const result: any = {};
    this.extractKV(data,
      ['vehicle','car','registration','vin','fin','engine','model','mileage','chassis','make','brand'],
      ['amount','total','date','time','invoice','tax']
    ).forEach(d => { result[d.key] = d.value; });
    return result;
  }

  extractServiceInfo(data: any): ServiceInfo {
    if (!data) return { services: [], tables: [] };
    const info: ServiceInfo = { services: [], tables: [] };
    const kws = ['service','labour','work','operation','description','code','qty','quantity','rate','amount'];

    this.findTables(data).forEach((table, idx) => {
      if (!Array.isArray(table) || !table.length || typeof table[0] !== 'object') return;
      const match = Object.keys(table[0]).filter(k =>
        kws.some(kw => k.toLowerCase().replace(/[^a-z0-9]/g, '').includes(kw))
      ).length;
      if (match < 2) return;
      const valid = table.filter((row: any) => {
        const wt = row['Work Order Type'] ?? row['Work_Order_Type'] ?? row['WorkOrderType'];
        return wt && !['Work Order Type','Work_Order_Type'].includes(wt) && wt !== null;
      });
      if (valid.length) {
        info.tables.push({ index: idx, rows: valid, totalServices: valid.length });
        info.services.push(...valid);
      }
    });
    return info;
  }

  // ── Shared utils ─────────────────────────────────────────────────────────────

  private fmt(amount: any): string {
    const n = parseFloat(String(amount).replace(/[^\d.-]/g, ''));
    return isNaN(n) ? '0.00'
      : new Intl.NumberFormat('en-IN', { minimumFractionDigits: 2, maximumFractionDigits: 2 }).format(n);
  }

  private truncate(v: any, max = 50): string {
    const s = String(v ?? 'N/A');
    return s.length > max ? s.substring(0, max) + '...' : s;
  }
}