"use client";

import { useState, useEffect } from "react";
import Link from "next/link";

export function Hero() {
  const [copied, setCopied] = useState(false);
  const [mounted, setMounted] = useState(false);
  const installCommand = "curl -L https://rindexer.xyz/install.sh | bash";

  useEffect(() => {
    setMounted(true);
  }, []);

  const copyToClipboard = async () => {
    await navigator.clipboard.writeText(installCommand);
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  };

  return (
    <section className="relative overflow-hidden bg-cream">
      {/* Animated background elements */}
      <div className="absolute inset-0 spotlight" />
      <div className="absolute inset-0 dot-pattern opacity-50" />

      {/* Floating decorative blobs */}
      <div className="absolute top-20 left-10 w-72 h-72 bg-rust-200/30 blob blob-animated animate-float blur-3xl" />
      <div className="absolute bottom-20 right-10 w-96 h-96 bg-rust-300/20 blob blob-animated animate-float-delayed blur-3xl" />
      <div className="absolute top-1/2 left-1/4 w-48 h-48 bg-amber-400/20 blob animate-float-delayed blur-2xl" />

      {/* Orbiting particles */}
      <div className="absolute top-1/2 left-1/2 -translate-x-1/2 -translate-y-1/2 w-[600px] h-[600px] pointer-events-none hidden lg:block">
        <div className="absolute inset-0 animate-spin-slow">
          <div className="absolute top-0 left-1/2 w-3 h-3 bg-rust-400 rounded-full blur-[2px]" />
          <div className="absolute bottom-0 left-1/2 w-2 h-2 bg-amber-400 rounded-full blur-[1px]" />
          <div className="absolute top-1/2 left-0 w-2 h-2 bg-rust-300 rounded-full blur-[1px]" />
        </div>
      </div>

      <div className="relative mx-auto max-w-7xl px-6 py-24 sm:py-32 lg:px-8 lg:py-40">
        <div className="mx-auto max-w-3xl text-center">
          {/* Badge */}
          <div
            className={`inline-flex items-center gap-2 rounded-full border-2 border-rust-200 bg-white/80 px-4 py-2 text-sm font-medium text-rust-700 shadow-sm backdrop-blur-sm mb-8 transition-all duration-700 ${
              mounted ? 'opacity-100 translate-y-0' : 'opacity-0 translate-y-4'
            }`}
          >
            <span className="relative flex h-2 w-2">
              <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-rust-400 opacity-75" />
              <span className="relative inline-flex rounded-full h-2 w-2 bg-rust-500" />
            </span>
            Built with Rust for maximum performance
          </div>

          {/* Main heading */}
          <h1
            className={`font-display text-5xl font-extrabold tracking-tight text-charcoal sm:text-6xl lg:text-7xl transition-all duration-700 delay-100 ${
              mounted ? 'opacity-100 translate-y-0' : 'opacity-0 translate-y-4'
            }`}
          >
            Blazing Fast{" "}
            <span className="gradient-text relative">
              EVM Indexing
              {/* Decorative underline */}
              <svg
                className="absolute -bottom-2 left-0 w-full h-3 text-rust-400/50"
                viewBox="0 0 200 12"
                preserveAspectRatio="none"
              >
                <path
                  d="M0,8 Q50,0 100,8 T200,8"
                  fill="none"
                  stroke="currentColor"
                  strokeWidth="3"
                  strokeLinecap="round"
                />
              </svg>
            </span>
          </h1>

          <p
            className={`mt-8 font-sans text-xl leading-relaxed text-gray-600 transition-all duration-700 delay-200 ${
              mounted ? 'opacity-100 translate-y-0' : 'opacity-0 translate-y-4'
            }`}
          >
            A no-code or framework solution to build high-performance EVM
            indexers. Configure with YAML, get instant GraphQL APIs, and stream
            to any destination.{" "}
            <span className="font-semibold text-rust-600">Open source and free forever.</span>
          </p>

          {/* Install command */}
          <div
            className={`mt-10 flex flex-col items-center gap-4 transition-all duration-700 delay-300 ${
              mounted ? 'opacity-100 translate-y-0' : 'opacity-0 translate-y-4'
            }`}
          >
            <div className="group relative w-full max-w-xl">
              {/* Glow effect on hover */}
              <div className="absolute -inset-1 bg-gradient-to-r from-rust-400 via-rust-500 to-amber-400 rounded-2xl blur-lg opacity-0 group-hover:opacity-30 transition-opacity duration-500" />

              <div className="terminal-window relative">
                <div className="terminal-header">
                  <div className="terminal-dot red" />
                  <div className="terminal-dot yellow" />
                  <div className="terminal-dot green" />
                  <span className="ml-3 text-xs text-gray-500 font-mono">terminal</span>
                </div>
                <div className="flex items-center font-mono text-sm">
                  <span className="select-none px-4 text-rust-400">$</span>
                  <code className="flex-1 py-4 pr-4 text-gray-100">
                    {installCommand}
                  </code>
                  <button
                    onClick={copyToClipboard}
                    className="border-l border-white/10 px-4 py-4 text-gray-400 transition-all hover:bg-white/5 hover:text-white active:scale-95"
                    title="Copy to clipboard"
                  >
                    {copied ? (
                      <svg className="h-5 w-5 text-green-400 animate-pop" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                        <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M5 13l4 4L19 7" />
                      </svg>
                    ) : (
                      <svg className="h-5 w-5 transition-transform group-hover:scale-110" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                        <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M8 16H6a2 2 0 01-2-2V6a2 2 0 012-2h8a2 2 0 012 2v2m-6 12h8a2 2 0 002-2v-8a2 2 0 00-2-2h-8a2 2 0 00-2 2v8a2 2 0 002 2z" />
                      </svg>
                    )}
                  </button>
                </div>
              </div>
            </div>

            {/* CTA Buttons */}
            <div
              className={`flex flex-col gap-4 sm:flex-row mt-4 transition-all duration-700 delay-400 ${
                mounted ? 'opacity-100 translate-y-0' : 'opacity-0 translate-y-4'
              }`}
            >
              <Link
                href="https://rindexer.xyz/docs/introduction/installation"
                className="magnetic-btn group relative overflow-hidden rounded-xl bg-gradient-to-r from-rust-500 to-rust-600 px-8 py-4 text-base font-semibold text-white shadow-lg shadow-rust-500/25 transition-all hover:shadow-xl hover:shadow-rust-500/30"
              >
                <span className="relative z-10 flex items-center justify-center gap-2">
                  Get Started
                  <svg className="w-4 h-4 transition-transform group-hover:translate-x-1" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M13 7l5 5m0 0l-5 5m5-5H6" />
                  </svg>
                </span>
                {/* Shimmer effect */}
                <div className="absolute inset-0 -translate-x-full group-hover:translate-x-full transition-transform duration-700 bg-gradient-to-r from-transparent via-white/20 to-transparent" />
              </Link>

              <Link
                href="https://github.com/joshstevens19/rindexer"
                className="magnetic-btn group flex items-center justify-center gap-2 rounded-xl border-2 border-charcoal/10 bg-white px-8 py-4 text-base font-semibold text-charcoal shadow-sm transition-all hover:border-rust-300 hover:bg-rust-50 hover:shadow-md"
              >
                <svg className="h-5 w-5 transition-transform group-hover:rotate-12" fill="currentColor" viewBox="0 0 24 24">
                  <path fillRule="evenodd" d="M12 2C6.477 2 2 6.484 2 12.017c0 4.425 2.865 8.18 6.839 9.504.5.092.682-.217.682-.483 0-.237-.008-.868-.013-1.703-2.782.605-3.369-1.343-3.369-1.343-.454-1.158-1.11-1.466-1.11-1.466-.908-.62.069-.608.069-.608 1.003.07 1.531 1.032 1.531 1.032.892 1.53 2.341 1.088 2.91.832.092-.647.35-1.088.636-1.338-2.22-.253-4.555-1.113-4.555-4.951 0-1.093.39-1.988 1.029-2.688-.103-.253-.446-1.272.098-2.65 0 0 .84-.27 2.75 1.026A9.564 9.564 0 0112 6.844c.85.004 1.705.115 2.504.337 1.909-1.296 2.747-1.027 2.747-1.027.546 1.379.202 2.398.1 2.651.64.7 1.028 1.595 1.028 2.688 0 3.848-2.339 4.695-4.566 4.943.359.309.678.92.678 1.855 0 1.338-.012 2.419-.012 2.747 0 .268.18.58.688.482A10.019 10.019 0 0022 12.017C22 6.484 17.522 2 12 2z" clipRule="evenodd" />
                </svg>
                View on GitHub
              </Link>
            </div>
          </div>

          {/* Stats preview */}
          <div
            className={`mt-16 flex flex-wrap items-center justify-center gap-8 transition-all duration-700 delay-500 ${
              mounted ? 'opacity-100 translate-y-0' : 'opacity-0 translate-y-4'
            }`}
          >
            <div className="flex items-center gap-2 text-sm text-gray-500">
              <svg className="w-5 h-5 text-amber-400" fill="currentColor" viewBox="0 0 20 20">
                <path d="M9.049 2.927c.3-.921 1.603-.921 1.902 0l1.07 3.292a1 1 0 00.95.69h3.462c.969 0 1.371 1.24.588 1.81l-2.8 2.034a1 1 0 00-.364 1.118l1.07 3.292c.3.921-.755 1.688-1.54 1.118l-2.8-2.034a1 1 0 00-1.175 0l-2.8 2.034c-.784.57-1.838-.197-1.539-1.118l1.07-3.292a1 1 0 00-.364-1.118L2.98 8.72c-.783-.57-.38-1.81.588-1.81h3.461a1 1 0 00.951-.69l1.07-3.292z" />
              </svg>
              <span className="font-semibold text-charcoal">1.2K+</span> GitHub Stars
            </div>
            <div className="w-px h-4 bg-gray-200" />
            <div className="flex items-center gap-2 text-sm text-gray-500">
              <span className="font-mono text-rust-500">v0.33.0</span> Latest Release
            </div>
            <div className="w-px h-4 bg-gray-200" />
            <div className="flex items-center gap-2 text-sm text-gray-500">
              <svg className="w-5 h-5 text-green-500" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 12l2 2 4-4m6 2a9 9 0 11-18 0 9 9 0 0118 0z" />
              </svg>
              MIT License
            </div>
          </div>
        </div>
      </div>

      {/* Bottom wave decoration */}
      <div className="absolute bottom-0 left-0 right-0 h-24 overflow-hidden">
        <svg
          className="absolute bottom-0 w-full h-24 text-white"
          viewBox="0 0 1440 74"
          fill="currentColor"
          preserveAspectRatio="none"
        >
          <path d="M0,32L60,37.3C120,43,240,53,360,58.7C480,64,600,64,720,58.7C840,53,960,43,1080,42.7C1200,43,1320,53,1380,58.7L1440,64L1440,74L1380,74C1320,74,1200,74,1080,74C960,74,840,74,720,74C600,74,480,74,360,74C240,74,120,74,60,74L0,74Z" />
        </svg>
      </div>
    </section>
  );
}
