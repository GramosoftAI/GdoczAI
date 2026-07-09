export const schema = {
   "@context": "https://schema.org",
  "@graph": [
    {
      "@type": "SoftwareApplication",
      "@id": "https://gramosoft.tech/gdoczai#software",
      "name": "GdoczAI",
      "applicationCategory": "BusinessApplication",
      "applicationSubCategory": "Intelligent Document Processing",
      "operatingSystem": "Cloud (SaaS), Self-hosted",
      "description":
        "GdoczAI is an AI document data extraction platform that turns invoices, PDFs, emails and scanned documents into clean, structured data automatically. GdoczAI Cloud lets teams go live in minutes with no model training. A self-hosted edition is available for enterprises with strict data-residency requirements.",
      "offers": [
        {
          "@type": "Offer",
          "name": "Free plan",
          "price": "0",
          "priceCurrency": "USD",
          "description":
            "Free tier to test GdoczAI Cloud with your own documents. No credit card required.",
        },
        {
          "@type": "Offer",
          "name": "Enterprise Self-Hosted",
          "priceCurrency": "INR",
          "description":
            "Annual license for on-premise, private cloud or air-gapped deployment.",
        },
      ],
      "publisher": { "@id": "https://gramosoft.tech#org" },
      "featureList": "AI invoice data extraction, PDF parsing, Email parsing, Table and line-item extraction, OCR for scans and handwriting, Data normalization and validation, ERP integrations (Odoo, ERPNext, SAP, Tally, Zoho Books), Google Sheets export, REST API, Webhooks, Human-in-the-loop review, Audit logs, Self-hosted enterprise edition"
    },
    {
      "@type": "Organization",
      "@id": "https://gramosoft.tech#org",
      "name": "Gramosoft Private Limited",
      "url": "https://gramosoft.tech",
      "logo": "https://gramosoft.tech/images/gramosoft-logo.png",
      "description":
        "Gramosoft is a Chennai-based deep-tech company building AI products for document processing (GdoczAI), web data extraction (GcrawlAI) and enterprise search (GsearchAI), with delivery presence in India, Singapore and Malaysia.",
      "address": {
        "@type": "PostalAddress",
        "addressLocality": "Chennai",
        "addressRegion": "Tamil Nadu",
        "addressCountry": "IN",
      },
      "sameAs": [
        "https://www.linkedin.com/company/gramosoft",
        "https://github.com/GramosoftAI",
      ],
    },
    {
      "@type": "WebPage",
      "@id": "https://gramosoft.tech/gdoczai#webpage",
      "url": "https://gramosoft.tech/gdoczai",
      "name": "GdoczAI — AI Document Data Extraction Software",
      "about": { "@id": "https://gramosoft.tech/gdoczai#software" },
      "breadcrumb": {
        "@type": "BreadcrumbList",
        "itemListElement": [
          { "@type": "ListItem", "position": 1, "name": "Gramosoft", "item": "https://gramosoft.tech" },
          { "@type": "ListItem", "position": 2, "name": "GdoczAI", "item": "https://gramosoft.tech/gdoczai"},
        ],
      },
    },
    {
      "@type": "FAQPage",
      "@id": "https://gramosoft.tech/gdoczai#faq",
      "mainEntity": [
        {
          "@type": "Question",
          "name": "What is GdoczAI?",
          "acceptedAnswer": {
            "@type": "Answer",
            "text": "GdoczAI is an AI document data extraction platform by Gramosoft. It turns invoices, PDFs, emails and scanned documents into clean, structured data and delivers it to tools like Google Sheets, Odoo, ERPNext, SAP, Tally and Zoho Books. GdoczAI Cloud requires no model training — most teams go live in minutes.",
          },
        },
        {
          "@type": "Question",
          "name": "How long does it take to get started with GdoczAI?",
          "acceptedAnswer": {
            "@type": "Answer",
            "text": "Most teams create a mailbox, upload a sample document and start extracting data within 10 minutes on GdoczAI Cloud. There is no model training or dataset preparation — you define your fields and start sending documents.",
          },
        },
        {
          "@type": "Question",
          "name": "What types of documents can GdoczAI handle?",
          "acceptedAnswer": {
            "@type": "Answer",
            "text": "GdoczAI processes invoices, purchase orders, delivery notes, bank statements, insurance forms, resumes, contracts and more. Supported inputs include PDFs, scanned images, email bodies and attachments, and spreadsheets — including multi-page documents, tables, stamps and handwriting.",
          },
        },
        {
          "@type": "Question",
          "name": "Do I need a credit card to try GdoczAI?",
          "acceptedAnswer": {
            "@type": "Answer",
            "text": "No. GdoczAI Cloud includes a free plan so you can sign up and test extraction with your own documents before committing. No payment information is required to start.",
          },
        },
        {
          "@type": "Question",
          "name": "Does GdoczAI offer a self-hosted version?",
          "acceptedAnswer": {
            "@type": "Answer",
            "text": "Yes. In addition to GdoczAI Cloud, an Enterprise Self-Hosted edition runs on your own servers, private cloud or fully air-gapped infrastructure — ideal for banks, insurers, airlines and regulated industries where documents cannot leave your environment.",
          },
        },
        {
          "@type": "Question",
          "name": "How does GdoczAI handle data privacy?",
          "acceptedAnswer": {
            "@type": "Answer",
            "text": "Your documents and extracted data are encrypted in transit and at rest, retention periods are configurable per mailbox, and customer data is never sold or used to train shared AI models. GdoczAI is designed to align with GDPR, India's DPDP Act and Singapore's PDPA.",
          },
        },
        {
          "@type": "Question",
          "name": "What happens when my document layouts change?",
          "acceptedAnswer": {
            "@type": "Answer",
            "text": "GdoczAI's AI extraction is instruction-based rather than trained on fixed samples, so it absorbs layout variations automatically. When an entirely new document type arrives, you adjust field settings in seconds — no retraining cycle.",
          },
        },
        {
          "@type": "Question",
          "name": "Can GdoczAI integrate with my ERP or accounting software?",
          "acceptedAnswer": {
            "@type": "Answer",
            "text": "Yes. GdoczAI delivers data to Odoo, ERPNext, SAP, Tally, Zoho Books and QuickBooks, exports to Google Sheets, Excel, CSV and JSON, and connects to any system through REST API and webhooks.",
          },
        },
        {
          "@type": "Question",
          "name": "How accurate is GdoczAI?",
          "acceptedAnswer": {
            "@type": "Answer",
            "text": "GdoczAI validates extracted data before delivery — dates, numbers, totals and reference formats are normalized and checked. In production workflows processing over 100,000 documents per month, most documents flow straight through, and exceptions are routed to a review queue instead of reaching your systems.",
          },
        },
        {
          "@type": "Question",
          "name": "Can GdoczAI handle high volumes?",
          "acceptedAnswer": {
            "@type": "Answer",
            "text": "Yes. GdoczAI is built for production use, from a handful of documents a day to hundreds of thousands per month, with automatic scaling, real-time delivery and full audit logs on every document.",
          },
        },
      ],
    },
  ]
}