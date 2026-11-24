import { defineUserConfig } from "vuepress";
import { hopeTheme } from "vuepress-theme-hope";
import { viteBundler } from '@vuepress/bundler-vite'

export default defineUserConfig({
  lang: "en-US",
  title: "SynTasks",
  description: "Async Distributed Task Manager",
  head: [
    [
      "meta",
      {
        property: "og:image",
        content: "https://syntasks.synlor.com/logo.svg",
      },
    ],
    ["link", { rel: "icon", href: "/favicon.svg", type: "image/svg+xml" }],
  ],

  bundler: viteBundler(),

  theme: hopeTheme({
    hostname: "https://syntasks.synlor.com/",
    logo: "/logo.svg",

    repo: "synlor/syntasks",
    docsBranch: "main",
    docsDir: "docs",

    navbarAutoHide: "none",
    sidebar: "structure",

    pure: true,
    focus: false,

    markdown: {
      tabs: true,
      mermaid: true
    },

    plugins: {
      readingTime: false,
      copyCode: {
        showInMobile: true,
      },

      sitemap: {
        changefreq: "daily",
        sitemapFilename: "sitemap.xml",
      },
      search: {
        maxSuggestions: 10,
      }
    },
  })
});
