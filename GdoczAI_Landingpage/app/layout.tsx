import type { Metadata } from "next";
import { AntdRegistry } from "@ant-design/nextjs-registry";
import { Plus_Jakarta_Sans} from "next/font/google";
import "bootstrap/dist/css/bootstrap.min.css";
import "./globals.css";
import { schema } from "./lib/schema"
import {GithubStarsProvider} from "../components/provider/GithubStarsProvider"

const plusJakartaSans = Plus_Jakarta_Sans({
  variable: "--font-plus-jakarta-sans",
  subsets: ["latin"],
});

// const geistMono = Geist_Mono({
//   variable: "--font-geist-mono",
//   subsets: ["latin"],
// });

const SITE_URL = "https://gramosoft.tech/gdoczai";

export const metadata: Metadata = {
  metadataBase: new URL("https://gramosoft.tech"),
  title:
    "GdoczAI — AI Document Data Extraction Software | Invoices, PDFs & Emails to Clean Data",
  description:
    "GdoczAI turns invoices, PDFs, emails and scans into clean, structured data automatically. Go live in minutes on GdoczAI Cloud — no model training needed. Self-hosted edition available for enterprises. Free to start.",
  keywords: [
    "AI document data extraction",
    "invoice OCR software",
    "PDF data extraction",
    "intelligent document processing",
    "email parser",
    "GdoczAI",
    "document automation",
    "IDP SaaS India",
  ],
  authors: [{ name: "Gramosoft Private Limited" }],
  alternates: { canonical: SITE_URL },
  robots: { index: true, follow: true, "max-snippet": -1, "max-image-preview": "large" } as any,
  openGraph: {
    type: "website",
    siteName: "GdoczAI by Gramosoft",
    title: "GdoczAI — Documents in. Clean data out. Automatically.",
    description:
      "AI data extraction that turns PDFs, emails and scans into production-ready data in minutes. No training. Free to start. Self-hosted option for enterprises.",
    url: SITE_URL,
    images: [{ url: "https://gramosoft.tech/images/gdoczai-og.png" }],
    locale: "en_IN",
  },
  twitter: {
    card: "summary_large_image",
    title: "GdoczAI — AI Document Data Extraction",
    description:
      "Turn invoices, PDFs and emails into clean data automatically. Cloud or self-hosted. Free to start.",
    images: ["https://gramosoft.tech/images/gdoczai-og.png"],
  },
};
export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en" className={`${plusJakartaSans.variable}`}>
      <head>
        <script
          type="application/ld+json"
          // eslint-disable-next-line react/no-danger
          dangerouslySetInnerHTML={{ __html: JSON.stringify(schema) }}
        />
        <script
          dangerouslySetInnerHTML={{
            __html: `
              (function() {
                function initReveal() {
                  const elements = document.querySelectorAll('.reveal');
                  if (elements.length === 0) return;
                  const observer = new IntersectionObserver((entries) => {
                    entries.forEach((entry) => {
                      if (entry.isIntersecting) {
                        entry.target.classList.add('active');
                        observer.unobserve(entry.target);
                      }
                    });
                  }, {
                    threshold: 0,
                    rootMargin: '0px 0px -60px 0px'
                  });
                  elements.forEach((el) => observer.observe(el));
                }
                if (document.readyState === 'loading') {
                  document.addEventListener('DOMContentLoaded', initReveal);
                } else {
                  initReveal();
                }
                const mutationObserver = new MutationObserver(initReveal);
                mutationObserver.observe(document.documentElement, { childList: true, subtree: true });
              })();
            `,
          }}
        />
      </head>
      <body>
        <AntdRegistry>
          <GithubStarsProvider>
            {children}
          </GithubStarsProvider>
        </AntdRegistry>
      </body>
    </html>
  );
}
