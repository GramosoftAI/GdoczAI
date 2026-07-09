# GdoczAI — Next.js (App Router) rebuild

Same page, same colors, same layout, same copy — rebuilt as a Next.js app using
**Ant Design** (Button, Slider, Collapse) and **Bootstrap 5** (grid/utility
classes), fully **responsive**, with **SEO + GEO + AEO** wired in.

## Run it

```bash
npm install
npm run dev      # http://localhost:3000
npm run build    # production build
npm run start    # serve the production build
```

## Rendering: SSG (with a note on SSR)

This site has **no per-request data** (no cookies, headers, query params, or
DB calls) — it's the same HTML for every visitor. So the whole page is
rendered to static HTML **at build time**:

- `app/page.tsx` has `export const dynamic = "force-static"`.
- After `npm run build` you'll see `○ (Static)` next to `/` in the build
  output — that's Next.js confirming it prerendered the route.
- The two interactive bits (the FAQ accordion and the ROI slider) are client
  components (`"use client"`) that hydrate in the browser — they don't force
  the *page* to be server-rendered per request, they just add interactivity
  on top of the static HTML.

**If you ever need SSR** (e.g. you later personalize the hero by IP/geo, or
read a cookie): just use `cookies()`/`headers()` from `next/headers`, or add
`export const dynamic = "force-dynamic"` to that route. Next.js's App Router
decides SSG vs SSR **per route** automatically based on whether the route
reads request-time data — you don't need a different framework or config for
it, just remove the `force-static` export and use a dynamic API.

## SEO / GEO / AEO

- **SEO** — `app/layout.tsx` exports Next's typed `Metadata`: title, meta
  description, keywords, canonical, robots, Open Graph, Twitter card.
- **GEO/AEO** (Generative/Answer-Engine Optimization — how AI answer engines
  like ChatGPT, Perplexity, Google AI Overviews find and quote your page) —
  a single JSON-LD `<script>` in `<head>` with a `@graph` of
  `SoftwareApplication`, `Organization`, `WebPage` + `BreadcrumbList`, and
  `FAQPage`. The visible "What is GdoczAI?" definition block and the FAQ
  section both mirror the JSON-LD text 1:1, which is what lets an AI engine
  lift a direct, accurate answer.

## Ant Design + Bootstrap

- `@ant-design/nextjs-registry` wraps the app so AntD's CSS-in-JS is
  collected server-side into the static HTML (no flash of unstyled
  content) — required for AntD v5 to work correctly with the App Router.
- `bootstrap/dist/css/bootstrap.min.css` is imported globally in
  `app/layout.tsx` for grid/utility classes (e.g. `d-none d-lg-flex` on the
  nav links). It's imported **before** `globals.css`, so the original
  hand-written styles always win on a specificity tie and the visual design
  is unchanged.
- AntD components used: `Button` (all CTAs, styled via the original
  `.btn/.btn-primary/.btn-outline/.btn-white` classes), `Slider` (ROI
  calculator), `Collapse` (FAQ — re-skinned in `globals.css` to look like the
  original `<details>` accordion).

## Structure

```
app/
  layout.tsx     — metadata, JSON-LD, font link, Bootstrap + AntD registry
  page.tsx        — assembles all sections (force-static)
  globals.css     — 1:1 port of the original page's CSS (colors, spacing, media queries)
components/
  Nav, Hero, TrustBar, Definition, Results, HowItWorks, Security,
  Production, Editions, UseCases, Integrations, TwoWays, Footer  — server components
  RoiCalculator.tsx, Faq.tsx                                     — client components
```

## Note

The Google Fonts `<link>` (Inter) needs outbound network access to
`fonts.googleapis.com`/`fonts.gstatic.com` at request time in the browser —
this is unrelated to SSG/SSR and works the same way the original static HTML
file loaded it.
