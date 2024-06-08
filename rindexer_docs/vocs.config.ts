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
      text: 'Start Building...',
      items: [
        {
          text: 'Project Types',
          link: '/docs/start-building/project-types',
          items: [
            { text: 'No-code', link: '/docs/start-building/project-types#no-code-project' },
            { text: 'Rust', link: '/docs/start-building/project-types#rust-project' }
          ],
        },
        {
          text: 'Create New Project',
          link: '/docs/server/api/introduction',
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
          text: 'Download ABI',
          link: '/docs/server/api/introduction',
        },
        {
          text: 'Running',
          link: '/docs/server/api/introduction',
          items: [
            { text: 'Indexer', link: '/docs/what-is-rrelayerr' },
            { text: 'GraphQL', link: '/docs/what-is-rrelayerr' },
            { text: 'All', link: '/docs/what-is-rrelayerr' },
          ],
        },
        {
          text: 'Codegen',
          link: '/docs/server/api/introduction',
          items: [
            { text: 'Typings', link: '/docs/what-is-rrelayerr' },
            { text: 'Indexer', link: '/docs/what-is-rrelayerr' },
            { text: 'All', link: '/docs/what-is-rrelayerr' },
          ],
        },
      ],
    },
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
