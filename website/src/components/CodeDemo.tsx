"use client";

import { useState, useEffect, useRef } from "react";

const tabs = [
  { id: "yaml", name: "YAML Config", icon: "M9 12h6m-6 4h6m2 5H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z" },
  { id: "graphql", name: "GraphQL Query", icon: "M4 7v10c0 2.21 3.582 4 8 4s8-1.79 8-4V7M4 7c0 2.21 3.582 4 8 4s8-1.79 8-4M4 7c0-2.21 3.582-4 8-4s8 1.79 8 4" },
  { id: "cli", name: "CLI Commands", icon: "M8 9l3 3-3 3m5 0h3M5 20h14a2 2 0 002-2V6a2 2 0 00-2-2H5a2 2 0 00-2 2v12a2 2 0 002 2z" },
];

const codeExamples = {
  yaml: `name: MyIndexer
description: Index ERC20 transfers
project_type: no-code

networks:
  - name: ethereum
    chain_id: 1
    rpc: https://eth.llamarpc.com

storage:
  postgres:
    enabled: true

contracts:
  - name: USDC
    details:
      - network: ethereum
        address: "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
        start_block: "18000000"
    abi: ./abis/erc20.json
    include_events:
      - Transfer`,
  graphql: `# Query indexed Transfer events
query GetTransfers {
  transfers(
    first: 10
    orderBy: blockNumber
    orderDirection: desc
  ) {
    id
    from
    to
    value
    blockNumber
    transactionHash
  }
}

# Filter by address
query GetTransfersByAddress($address: String!) {
  transfers(where: { from: $address }) {
    to
    value
    blockNumber
  }
}`,
  cli: `# Install rindexer
curl -L https://rindexer.xyz/install.sh | bash

# Create a new project
rindexer new my-indexer --type no-code

# Start indexing with GraphQL API
rindexer start all

# Or start just the indexer
rindexer start indexer

# Or start just the GraphQL API
rindexer start graphql

# Add a new contract
rindexer add contract

# Generate code for Rust projects
rindexer codegen typings`,
};

export function CodeDemo() {
  const [activeTab, setActiveTab] = useState("yaml");
  const [isVisible, setIsVisible] = useState(false);
  const [copied, setCopied] = useState(false);
  const sectionRef = useRef<HTMLElement>(null);

  useEffect(() => {
    const observer = new IntersectionObserver(
      ([entry]) => {
        if (entry.isIntersecting) {
          setIsVisible(true);
        }
      },
      { threshold: 0.1 }
    );

    if (sectionRef.current) {
      observer.observe(sectionRef.current);
    }

    return () => observer.disconnect();
  }, []);

  const copyCode = async () => {
    await navigator.clipboard.writeText(codeExamples[activeTab as keyof typeof codeExamples]);
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  };

  return (
    <section ref={sectionRef} className="relative bg-cream py-24 sm:py-32 overflow-hidden">
      {/* Background elements */}
      <div className="absolute inset-0 dot-pattern opacity-30" />
      <div className="absolute top-1/2 left-1/2 -translate-x-1/2 -translate-y-1/2 w-[800px] h-[800px] bg-gradient-radial from-rust-100/30 to-transparent rounded-full blur-3xl" />

      <div className="relative mx-auto max-w-7xl px-6 lg:px-8">
        <div
          className={`mx-auto max-w-2xl text-center transition-all duration-700 ${
            isVisible ? "opacity-100 translate-y-0" : "opacity-0 translate-y-8"
          }`}
        >
          <div className="inline-flex items-center gap-2 rounded-full bg-white border-2 border-rust-100 px-4 py-1.5 text-sm font-medium text-rust-600 mb-4 shadow-sm">
            <span className="relative flex h-2 w-2">
              <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-rust-400 opacity-75" />
              <span className="relative inline-flex rounded-full h-2 w-2 bg-rust-500" />
            </span>
            Developer experience
          </div>
          <h2 className="font-display text-3xl font-bold tracking-tight text-charcoal sm:text-4xl lg:text-5xl">
            Simple to configure,{" "}
            <span className="gradient-text">powerful to use</span>
          </h2>
          <p className="mt-6 text-lg text-gray-600 leading-relaxed">
            Configure your indexer with YAML, query with GraphQL, and manage
            with the CLI.
          </p>
        </div>

        <div
          className={`mx-auto mt-16 max-w-4xl transition-all duration-700 delay-200 ${
            isVisible ? "opacity-100 translate-y-0" : "opacity-0 translate-y-8"
          }`}
        >
          {/* Code window */}
          <div className="group relative">
            {/* Glow effect */}
            <div className="absolute -inset-1 bg-gradient-to-r from-rust-400 via-rust-500 to-amber-400 rounded-3xl blur-xl opacity-20 group-hover:opacity-30 transition-opacity duration-500" />

            <div className="relative overflow-hidden rounded-2xl border-2 border-charcoal/10 bg-midnight shadow-2xl">
              {/* Window header */}
              <div className="flex items-center justify-between border-b border-white/10 bg-gradient-to-b from-white/10 to-white/5 px-4 py-3">
                <div className="flex items-center gap-2">
                  <div className="flex gap-2">
                    <div className="w-3 h-3 rounded-full bg-[#ff5f56] hover:scale-110 transition-transform cursor-pointer" />
                    <div className="w-3 h-3 rounded-full bg-[#ffbd2e] hover:scale-110 transition-transform cursor-pointer" />
                    <div className="w-3 h-3 rounded-full bg-[#27c93f] hover:scale-110 transition-transform cursor-pointer" />
                  </div>
                </div>

                {/* Copy button */}
                <button
                  onClick={copyCode}
                  className="flex items-center gap-2 px-3 py-1.5 rounded-lg text-xs font-medium text-gray-400 hover:text-white hover:bg-white/10 transition-all"
                >
                  {copied ? (
                    <>
                      <svg className="w-4 h-4 text-green-400" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                        <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M5 13l4 4L19 7" />
                      </svg>
                      Copied!
                    </>
                  ) : (
                    <>
                      <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                        <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M8 16H6a2 2 0 01-2-2V6a2 2 0 012-2h8a2 2 0 012 2v2m-6 12h8a2 2 0 002-2v-8a2 2 0 00-2-2h-8a2 2 0 00-2 2v8a2 2 0 002 2z" />
                      </svg>
                      Copy
                    </>
                  )}
                </button>
              </div>

              {/* Tabs */}
              <div className="flex border-b border-white/10 bg-white/5">
                {tabs.map((tab) => (
                  <button
                    key={tab.id}
                    onClick={() => setActiveTab(tab.id)}
                    className={`group/tab relative flex items-center gap-2 px-5 py-3 text-sm font-medium transition-all ${
                      activeTab === tab.id
                        ? "text-rust-400"
                        : "text-gray-500 hover:text-gray-300"
                    }`}
                  >
                    <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                      <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d={tab.icon} />
                    </svg>
                    {tab.name}
                    {/* Active indicator */}
                    {activeTab === tab.id && (
                      <div className="absolute bottom-0 left-0 right-0 h-0.5 bg-gradient-to-r from-rust-400 to-rust-600" />
                    )}
                    {/* Hover indicator */}
                    <div className={`absolute bottom-0 left-0 right-0 h-0.5 bg-white/20 scale-x-0 group-hover/tab:scale-x-100 transition-transform ${activeTab === tab.id ? "hidden" : ""}`} />
                  </button>
                ))}
              </div>

              {/* Code content */}
              <div className="relative p-0 min-h-[400px]">
                <pre className="border-0 rounded-none bg-transparent m-0">
                  <code className="block p-6 text-sm leading-relaxed text-gray-100 font-mono">
                    {codeExamples[activeTab as keyof typeof codeExamples].split('\n').map((line, i) => (
                      <div
                        key={i}
                        className="hover:bg-white/5 -mx-6 px-6 transition-colors"
                      >
                        <span className="inline-block w-8 text-gray-600 select-none text-right mr-4">
                          {i + 1}
                        </span>
                        {line.startsWith('#') ? (
                          <span className="text-gray-500">{line}</span>
                        ) : line.includes(':') && !line.trim().startsWith('-') && !line.includes('//') ? (
                          <>
                            <span className="text-cyan-400">{line.split(':')[0]}</span>
                            <span className="text-gray-400">:</span>
                            <span className="text-amber-300">{line.split(':').slice(1).join(':')}</span>
                          </>
                        ) : line.trim().startsWith('-') ? (
                          <>
                            <span className="text-rust-400">{line.match(/^\s*/)?.[0]}- </span>
                            <span className="text-amber-300">{line.trim().slice(2)}</span>
                          </>
                        ) : line.includes('$') ? (
                          <>
                            <span className="text-rust-400">$ </span>
                            <span className="text-gray-100">{line.replace('$ ', '')}</span>
                          </>
                        ) : (
                          <span>{line}</span>
                        )}
                      </div>
                    ))}
                  </code>
                </pre>
              </div>
            </div>
          </div>
        </div>
      </div>
    </section>
  );
}
