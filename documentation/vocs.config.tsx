import { defineConfig } from "vocs";

export default defineConfig({
  head: (
    <>
      <meta property="og:type" content="website" />
      <meta
        property="og:title"
        content="rindexer · A lighting-fast multi chain indexing solution written in Rust"
      />
      <meta property="og:image" content="https://rindexer.xyz/favicon.png" />
      <meta property="og:url" content="https://rindexer.xyz" />
      <meta
        property="og:description"
        content="Build scalable, efficient, and secure blockchain indexing solutions for modern decentralized applications."
      />
    </>
  ),
  title: "🦀 rindexer 🦀",
  iconUrl: "/favicon.png",
  ogImageUrl: "/favicon.png",
  description:
    "rindexer is a lighting-fast multi chain indexing solution written in Rust",

  topNav: [
    { text: "Docs", link: "/docs/introduction/installation", match: "/docs" },
    { text: "Changelog", link: "/docs/changelog", match: "/docs" },
  ],
  socials: [
    {
      icon: "github",
      link: "https://github.com/joshstevens19/rindexer",
    },
  ],
  sidebar: [
    {
      text: "Introduction",
      items: [
        {
          text: "What is rindexer?",
          link: "/docs/introduction/what-is-rindexer",
        },
        { text: "Why rindexer?", link: "/docs/introduction/why-rindexer" },
        { text: "Installation", link: "/docs/introduction/installation" },
        {
          text: "Other Indexing Tools",
          link: "/docs/introduction/other-indexing-tools",
        },
      ],
    },
    {
      text: "Start Building...",
      items: [
        {
          text: "Project Types",
          link: "/docs/start-building/project-types",
          items: [
            {
              text: "No-code",
              link: "/docs/start-building/project-types/no-code-project",
            },
            {
              text: "Rust",
              link: "/docs/start-building/project-types/rust-project",
            },
          ],
        },
        {
          text: "Create New Project",
          link: "/docs/start-building/create-new-project",
        },
        {
          text: "Live indexing vs Historic indexing",
          link: "/docs/start-building/live-indexing-and-historic",
        },
        {
          text: "YAML Config",
          link: "/docs/start-building/yaml-config",
          items: [
            {
              text: "Top-level Fields",
              link: "/docs/start-building/yaml-config/top-level-fields",
            },
            {
              text: "Networks",
              link: "/docs/start-building/yaml-config/networks",
            },
            {
              text: "Storage",
              link: "/docs/start-building/yaml-config/storage",
            },
            {
              text: "Contracts",
              link: "/docs/start-building/yaml-config/contracts",
            },
            {
              text: "GraphQL",
              link: "/docs/start-building/yaml-config/graphql",
            },
            { text: "Global", link: "/docs/start-building/yaml-config/global" },
          ],
        },
        {
          text: "Add",
          link: "/docs/start-building/add",
        },
        {
          text: "Codegen",
          link: "/docs/start-building/codegen",
        },
        {
          text: "Running",
          link: "/docs/start-building/running",
        },
        {
          text: "Delete",
          link: "/docs/start-building/delete",
        },
        {
          text: "Chatbots",
          link: "/docs/start-building/chatbots",
          items: [
            {
              text: "Telegram",
              link: "/docs/start-building/chatbots/telegram",
            },
            { text: "Discord", link: "/docs/start-building/chatbots/discord" },
            { text: "Slack", link: "/docs/start-building/chatbots/slack" },
          ],
        },
        {
          text: "Streams",
          link: "/docs/start-building/streams",
          items: [
            { text: "Webhooks", link: "/docs/start-building/streams/webhooks" },
            { text: "Kafka", link: "/docs/start-building/streams/kafka" },
            { text: "Rabbitmq", link: "/docs/start-building/streams/rabbitmq" },
            { text: "SNS/SQS", link: "/docs/start-building/streams/sns" },
            { text: "Redis", link: "/docs/start-building/streams/redis" },
          ],
        },
        {
          text: "Phantom Events",
          link: "/docs/start-building/phantom",
        },
        {
          text: "Rust Project Deep Dive",
          link: "/docs/start-building/rust-project-deep-dive",
          items: [
            {
              text: "Typings",
              link: "/docs/start-building/rust-project-deep-dive/typings",
            },
            {
              text: "Indexers",
              link: "/docs/start-building/rust-project-deep-dive/indexers",
            },
            { text: "Building Own DB Schema - coming soon" },
          ],
        },
      ],
    },
    {
      text: "Accessing Data",
      items: [
        { text: "GraphQL", link: "/docs/accessing-data/graphql" },
        { text: "Direct SQL", link: "/docs/accessing-data/direct-sql" },
      ],
    },
    {
      text: "Deploying",
      items: [
        { text: "Railway", link: "/docs/deploying/railway" },
        { text: "AWS", link: "/docs/deploying/aws" },
        { text: "GCP", link: "/docs/deploying/gcp" },
      ],
    },
    {
      text: "References",
      items: [
        { text: "CLI ", link: "/docs/references/cli" },
        {
          text: "RPC Node Providers ",
          link: "/docs/references/rpc-node-providers",
        },
      ],
    },
    {
      text: "Advanced",
      items: [
        { text: "From A Foundry Project - coming soon" },
        { text: "From A Hardhat Project - coming soon" },
        { text: "Using Reth ExExes - coming soon" },
      ],
    },
    {
      text: "Coming soon",
      items: [
        {
          text: "log block timestamps",
          link: "/docs/coming-soon/log-block-timestamps",
        },
      ],
    },
    { text: "Changelog", link: "/docs/changelog" },
    { text: "Shoutout", link: "/docs/shoutout" },
  ],
});
