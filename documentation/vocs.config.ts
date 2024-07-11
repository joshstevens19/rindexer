import { defineConfig } from 'vocs'

export default defineConfig({
  title: 'rindexer',
  iconUrl: '/favicon-32x32.png',
  topNav: [
    { text: 'Docs', link: '/docs/introduction/installation', match: '/docs' },
    {
      text: '0.1.0-beta',
      items: [
        {
          text: 'Changelog',
          link: '/docs/changelog',
        },
        // {
        //   text: 'Contributing',
        //   link: 'https://github.com/wevm/vocs/blob/main/.github/CONTRIBUTING.md',
        // },
      ],
    },
  ],
  socials: [
    {
      icon: 'github',
      link: 'https://github.com/joshstevens19/rindexer',
    },
  ],
  sidebar: [
    {
      text: 'Introduction',
      items: [
        { text: 'What is rindexer?', link: '/docs/introduction/what-is-rindexer' },
        { text: 'Why rindexer?', link: '/docs/introduction/why-rindexer' },
        { text: 'Installation', link: '/docs/introduction/installation' },
        { text: 'Other Indexing Tools', link: '/docs/introduction/other-indexing-tools' },
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
          text: 'Live indexing vs Historic indexing',
          link: '/docs/start-building/live-indexing-and-historic',
        },
        {
          text: 'YAML Config',
          link: '/docs/start-building/yaml-config',
          items: [
            { text: 'Top-level Fields', link: '/docs/start-building/yaml-config/top-level-fields' },
            { text: 'Networks', link: '/docs/start-building/yaml-config/networks' },
            { text: 'Storage', link: '/docs/start-building/yaml-config/storage' },
            { text: 'Contracts', link: '/docs/start-building/yaml-config/contracts' },
            { text: 'GraphQL', link: '/docs/start-building/yaml-config/graphql' },
            { text: 'Global', link: '/docs/start-building/yaml-config/global' },
          ],
        },
        {
          text: 'Add',
          link: '/docs/start-building/add',
        },
        {
          text: 'Codegen',
          link: '/docs/start-building/codegen',
        },
        {
          text: 'Running',
          link: '/docs/start-building/running',
        },
        {
          text: 'Delete',
          link: '/docs/start-building/delete',
        },
        {
          text: 'Rust Project Deep Dive',
          link: '/docs/start-building/rust-project-deep-dive',
          items: [
            { text: 'Typings', link: '/docs/start-building/rust-project-deep-dive/typings' },
            { text: 'Indexers', link: '/docs/start-building/rust-project-deep-dive/indexers' },
            { text: 'Building Own DB Schema - coming soon' },
          ],
        },
      ],
    },
    {
      text: 'Accessing Data',
      items: [
        { text: 'GraphQL', link: '/docs/accessing-data/graphql' },
        { text: 'Direct SQL', link: '/docs/accessing-data/direct-sql' },
      ],
    },
    {
      text: 'Deploying',
      items: [
        { text: 'Railway - coming soon' },
        { text: 'AWS - coming soon' },
        { text: 'GCP - coming soon' },
      ],
    },
    {
      text: 'References',
      items: [
        { text: 'CLI ', link: '/docs/references/cli' },
        { text: 'RPC Node Providers ', link: '/docs/references/rpc-node-providers' },
      ],
    },
    {
      text: 'Advanced',
      items: [
        { text: 'From A Foundry Project - coming soon' },
        { text: 'From A Hardhat Project - coming soon' },
        { text: 'Using Reth ExExes - coming soon' },
      ],
    },
    {
      text: 'Coming soon',
      items: [
        { text: 'log block timestamps', link: '/docs/coming-soon/log-block-timestamps' }
      ],
    },
    { text: 'Changelog', link: '/docs/changelog' },
  ],
})
