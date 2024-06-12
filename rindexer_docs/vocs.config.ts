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
            { text: 'No-code', link: '/docs/start-building/project-types/no-code-project' },
            { text: 'Rust', link: '/docs/start-building/project-types/rust-project' }
          ],
        },
        {
          text: 'Create New Project',
          link: '/docs/start-building/create-new-project',
        },
        {
          text: 'YAML Config',
          link: '/docs/start-building/yaml-config',
          items: [
            { text: 'Top-level fields', link: '/docs/start-building/yaml-config/top-level-fields' },
            { text: 'Networks', link: '/docs/start-building/yaml-config/networks' },
            { text: 'Storage', link: '/docs/start-building/yaml-config/storage' },
            { text: 'Indexers', link: '/docs/start-building/yaml-config/indexers' },
            { text: 'Global', link: '/docs/start-building/yaml-config/global' },
          ],
        },
        {
          text: 'Download ABI',
          link: '/docs/start-building/download-abi',
        },
        {
          text: 'Running',
          link: '/docs/start-building/running',
        },
        {
          text: 'Rust Project Deep Dive',
          link: '/docs/start-building/rust-project-deep-dive',
          items: [
            { text: 'Typings', link: '/docs/start-building/rust-project-deep-dive/typings' },
            { text: 'Indexers', link: '/docs/start-building/rust-project-deep-dive/indexers' },
            { text: 'All', link: '/docs/start-building/rust-project-deep-dive/all' },
          ],
        },
        // {
        //   text: 'Codegen',
        //   link: '/docs/server/api/introduction',
        //   items: [
        //     { text: 'Typings', link: '/docs/what-is-rrelayerr' },
        //     { text: 'Indexer', link: '/docs/what-is-rrelayerr' },
        //     { text: 'All', link: '/docs/what-is-rrelayerr' },
        //   ],
        // },
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
