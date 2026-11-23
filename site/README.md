# Landing page

The Kian landing page: plain HTML, CSS and JavaScript. No build step, no external requests, no
cookies, no client-side framework.

## Preview

```bash
python3 -m http.server 8080 --directory site
```

Open <http://127.0.0.1:8080>. `_headers` is a Cloudflare Pages file, so the local server does not
apply the CSP; that is expected.

## Deploy to Cloudflare Pages

Direct upload, no repository connection needed. The first run prompts to create the project:

```bash
npx wrangler pages deploy site --project-name kian-site
```

Or connect the repository in the dashboard: framework preset **None**, build command empty, build
output directory `site`. `_headers` applies the CSP, HSTS and cache policy on every deploy.

Bind a custom domain under **Custom domains** once DNS points at Cloudflare.

## Live

<https://kian-site.pages.dev/> (Cloudflare Pages, project `kian-site`). Every deploy uploads `site/`
as-is; `_headers` supplies the CSP, HSTS and cache policy. There is no GitHub Pages mirror — the
Cloudflare project is the only deployment, and no workflow publishes this directory.

## Before going live

- Replace `https://kian-site.pages.dev` in `index.html` (canonical plus Open Graph/Twitter URLs)
  with the production origin once a custom domain is bound. The current value matches the default
  `wrangler` project name.
- Replace `flyer103/kian-runtime` if the repository moves.
- Regenerate `assets/og.png` when the headline or one-liner changes.
- Cloudflare Web Analytics is enabled on the Pages project, so the platform injects its beacon into
  the deployed HTML; `_headers` allows exactly that host (`static.cloudflareinsights.com` for the
  script, `cloudflareinsights.com` for the beacon request). Any other third-party script still needs
  the CSP widened first.

## Notes

- The page mirrors the claims in the root `README.md`; when the README's features or quickstart
  change, update `index.html` in the same change. The recorded demo GIF stays in the README rather
  than being duplicated here.
- Repository convention: all text is English.
- Unknown nested paths (`/a/b`) get `404.html`, whose relative assets resolve against the requested
  path; the site is a single page, so this trade-off is accepted.
