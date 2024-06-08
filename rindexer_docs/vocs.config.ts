import { defineConfig } from 'vocs'

export default defineConfig({
  title: 'Docs',
  sidebar: [
    {
      text: 'Introduction',
      items: [
        { text: 'What is rindexer?', link: '/docs/introduction/what-is-rindexer' },
        { text: 'Why rindexer?', link: '/docs/introduction/why-rindexer' },
        { text: 'Installation', link: '/docs/introduction/installation' },
      ],
    },
    {
      text: 'Using rindexer',
      items: [
        {
          text: 'Getting started',
          items: [
            {
              text: 'No-code vs rust project',
              link: '/docs/server/api/introduction',
            },
            {
              text: 'No-code',
              link: '/docs/server/api/introduction',
            },
            {
              text: 'Rust project',
              link: '/docs/server/api/introduction',
            }
          ],
        },
      ],
    },
    {
      text: 'YAML Config',
      link: '/docs/what-is-rrelayerr',
      items: [
        { text: 'Top-level fields', link: '/docs/what-is-rrelayerr' },
        { text: 'Indexers', link: '/docs/what-is-rrelayerr' },
        { text: 'Networks', link: '/docs/what-is-rrelayerr' },
        { text: 'Global', link: '/docs/what-is-rrelayerr' },
        { text: 'Storage', link: '/docs/what-is-rrelayerr' },
      ],
    },
    {
      text: 'Running',
      items: [
        { text: 'Indexer', link: '/docs/what-is-rrelayerr' },
        { text: 'GraphQL', link: '/docs/what-is-rrelayerr' },
        { text: 'All', link: '/docs/what-is-rrelayerr' },
      ],
    },
    { text: 'Download ABIs', link: '/docs/what-is-rrelayerr' },
    {
      text: 'Accessing Data',
      items: [
        { text: 'GraphQL', link: '/docs/what-is-rrelayerr' },
        { text: 'Direct SQL', link: '/docs/what-is-rrelayerr' },
      ],
    },
    {
      text: 'Deploying',
      items: [
        { text: 'AWS', link: '/docs/what-is-rrelayerr' },
        { text: 'GCP', link: '/docs/what-is-rrelayerr' },
        { text: 'Railway', link: '/docs/what-is-rrelayerr' },
      ],
    },
    {
      text: 'Advanced',
      items: [
        { text: 'Building Own DB Schema ', link: '/docs/what-is-rrelayerr' },
        { text: 'From A Foundry Project', link: '/docs/what-is-rrelayerr' },
        { text: 'From A Hardhat Project', link: '/docs/what-is-rrelayerr' },
      ],
    },
    { text: 'Changelog', link: '/docs/what-is-rrelayerr' },
  ],
})
