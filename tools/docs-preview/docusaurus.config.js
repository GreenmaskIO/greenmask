// Local preview of the docs/ tree with the stock Docusaurus theme. The published
// site (greenmaskio.github.io) has its own config and styles.
const path = require('path');

const docsDir = path.resolve(__dirname, '../../docs');

/** @type {import('@docusaurus/types').Config} */
module.exports = {
  title: 'Greenmask docs preview',
  url: 'http://localhost',
  baseUrl: '/',
  // Links to other products of the site (e.g. /docs/arcave/) do not exist here.
  onBrokenLinks: 'warn',
  onBrokenAnchors: 'warn',
  markdown: {
    hooks: {
      onBrokenMarkdownLinks: 'throw',
    },
  },
  presets: [
    [
      'classic',
      {
        docs: {
          path: docsDir,
          sidebarPath: path.join(docsDir, 'sidebars.json'),
          routeBasePath: '/',
        },
        blog: false,
        pages: false,
      },
    ],
  ],
  themeConfig: {
    navbar: {title: 'Greenmask docs preview'},
  },
};
