import { defineConfig } from 'vocs/config'

export default defineConfig({
  title: 'Headrace',
  description: 'OTel-native, stateful stream processing in a single Rust binary.',
  baseUrl: 'https://headrace.rs',
  // Docs live under /docs; the hand-rolled landing (site/) owns the root.
  basePath: '/docs',
  // Prerender every page to static HTML so it deploys to a static host (Cloudflare Pages).
  renderStrategy: 'full-static',
  logoUrl: { light: '/logo-wordmark.svg', dark: '/logo-wordmark-dark.svg' },
  iconUrl: '/favicon.svg',
  accentColor: 'light-dark(#0E9AA0, #2DD4BF)',
  // Brand theme: retint Vocs' neutral surfaces, code blocks, and borders to the
  // pipeline-diagram palette (Paper / near-black page, white / #131C23 surfaces,
  // brand borders), plus the landing hero styles and a copy-button fix.
  head: {
    style: [
      {
        innerHTML: `
:root,:host{
  --vocs-background-color-primary: light-dark(#F5F7F9,#0B1014);
  --vocs-background-color-surface: light-dark(#FFFFFF,#131C23);
  --vocs-background-color-surfaceMuted: light-dark(#EDF1F3,#0F1720);
  --vocs-background-color-surfaceTint: light-dark(#E9EEF1,#18232C);
  --vocs-background-color-code-block: light-dark(#FFFFFF,#18242F);
  --vocs-background-color-inline-code: light-dark(#EDF1F3,#1B2A38);
  --vocs-background-color-code-highlighted: light-dark(#E6F0EF,#12333A);
  --vocs-border-color-primary: light-dark(#DCE4E8,#243642);
  --vocs-border-color-secondary: light-dark(#E4EAED,#1C2831);
  --vocs-color-gray12: #DCE4E8;
  --vocs-color-gray4: #131C23;
}
/* copy button: pin top-right even on single-line blocks. Tailwind v4 uses the
   translate property (not transform), so reset that too. */
button[data-single-line="true"][class]{top:.625rem!important;translate:none!important;transform:none!important;--tw-translate-y:0!important}
`,
      },
    ],
  },
  topNav: [
    { text: 'Getting started', link: '/getting-started' },
    { text: 'GitHub', link: 'https://github.com/headrace-rs/headrace' },
  ],
  sidebar: [
    { text: 'Overview', link: '/' },
    { text: 'Getting started', link: '/getting-started' },
    {
      text: 'Transforms',
      collapsed: false,
      items: [
        { text: 'filter', link: '/transforms/filter' },
        { text: 'map', link: '/transforms/map' },
        { text: 'window', link: '/transforms/window' },
      ],
    },
  ],
})
