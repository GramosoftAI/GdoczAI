import React from "react";

const faqs = [
  { q: "What is GdoczAI?", a: "GdoczAI is an AI document data extraction platform by Gramosoft. It turns invoices, PDFs, emails and scanned documents into clean, structured data and delivers it to tools like Google Sheets, Odoo, ERPNext, SAP, Tally and Zoho Books. GdoczAI Cloud requires no model training — most teams go live in minutes." },
  { q: "How long does it take to get started?", a: "Most teams create a mailbox, upload a sample document and start extracting data within 10 minutes on GdoczAI Cloud. There's no model training or dataset preparation — you define your fields and start sending documents." },
  { q: "What types of documents can GdoczAI handle?", a: "Invoices, purchase orders, delivery notes, bank statements, insurance forms, resumes, contracts and more. Supported inputs include PDFs, scanned images, email bodies and attachments, and spreadsheets — including multi-page documents, tables, stamps and handwriting." },
  { q: "Do I need a credit card to try GdoczAI?", a: "No. GdoczAI Cloud includes a free plan so you can sign up and test extraction with your own documents before committing. No payment information is required." },
  { q: "Does GdoczAI offer a self-hosted version?", a: "Yes. In addition to GdoczAI Cloud, an Enterprise Self-Hosted edition runs on your own servers, private cloud or fully air-gapped infrastructure — ideal for banks, insurers, airlines and regulated industries where documents cannot leave your environment." },
  { q: "How does GdoczAI handle data privacy?", a: "Documents and extracted data are encrypted in transit and at rest, retention periods are configurable per mailbox, and customer data is never sold or used to train shared AI models. GdoczAI is designed to align with GDPR, India's DPDP Act and Singapore's PDPA." },
  { q: "What happens when document formats change?", a: "Extraction is instruction-based rather than trained on fixed samples, so GdoczAI absorbs layout variations automatically. When an entirely new document type arrives, adjust field settings in seconds — no retraining cycle." },
  { q: "Can GdoczAI integrate with my ERP or accounting software?", a: "Yes. GdoczAI delivers data to Odoo, ERPNext, SAP, Tally, Zoho Books and QuickBooks, exports to Google Sheets, Excel, CSV and JSON, and connects to any system through REST API and webhooks." },
  { q: "How accurate is GdoczAI?", a: "Extracted data is validated before delivery — dates, numbers, totals and reference formats are normalized and checked. In production workflows processing over 100,000 documents per month, most documents flow straight through, with exceptions routed to a review queue instead of reaching your systems." },
  { q: "Can GdoczAI handle high volumes or business-critical workflows?", a: "Yes. GdoczAI is built for production use, from a handful of documents a day to hundreds of thousands per month, with automatic scaling, real-time delivery and full audit logs on every document." },
];

export default function Faq() {
  return (
    <section id="faq" aria-label="Frequently asked questions">
      <div className="wrap">
        <div className="center">
          <span className="kicker">FAQ</span>
          <h2>Frequently asked questions</h2>
        </div>
        <div className="faq-list">
          {faqs.map((f, i) => (
            <details className="faq-item" key={i} open={i === 0 ? true : undefined}>
              <summary className="faq-question">
                <h3>{f.q}</h3>
                <span className="faq-icon">
                  <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round">
                    <line x1="12" y1="5" x2="12" y2="19"></line>
                    <line x1="5" y1="12" x2="19" y2="12"></line>
                  </svg>
                </span>
              </summary>
              <div className="faq-answer">
                <p>{f.a}</p>
              </div>
            </details>
          ))}
        </div>
      </div>
    </section>
  );
}
