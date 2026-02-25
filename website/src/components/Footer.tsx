"use client";

import Link from "next/link";
import { useEffect, useRef, useState } from "react";

const navigation = {
  product: [
    {
      name: "Documentation",
      href: "https://rindexer.xyz/docs/introduction/installation",
    },
    {
      name: "Quick Start",
      href: "https://rindexer.xyz/docs/start-building/no-code",
    },
    {
      name: "Examples",
      href: "https://github.com/joshstevens19/rindexer/tree/master/examples",
    },
    { name: "Changelog", href: "https://rindexer.xyz/docs/changelog" },
  ],
  resources: [
    {
      name: "YAML Configuration",
      href: "https://rindexer.xyz/docs/references/yaml-config",
    },
    { name: "GraphQL API", href: "https://rindexer.xyz/docs/references/graphql" },
    {
      name: "CLI Reference",
      href: "https://rindexer.xyz/docs/references/cli-commands",
    },
    { name: "Rust Framework", href: "https://rindexer.xyz/docs/start-building/project" },
  ],
  deployment: [
    { name: "Railway", href: "https://rindexer.xyz/docs/deploying/railway" },
    { name: "AWS", href: "https://rindexer.xyz/docs/deploying/aws" },
    { name: "GCP", href: "https://rindexer.xyz/docs/deploying/gcp" },
    { name: "Docker", href: "https://rindexer.xyz/docs/deploying/docker" },
  ],
  community: [
    { name: "GitHub", href: "https://github.com/joshstevens19/rindexer" },
    {
      name: "Issues",
      href: "https://github.com/joshstevens19/rindexer/issues",
    },
    {
      name: "Discussions",
      href: "https://github.com/joshstevens19/rindexer/discussions",
    },
  ],
};

export function Footer() {
  const [isVisible, setIsVisible] = useState(false);
  const footerRef = useRef<HTMLElement>(null);

  useEffect(() => {
    const observer = new IntersectionObserver(
      ([entry]) => {
        if (entry.isIntersecting) {
          setIsVisible(true);
        }
      },
      { threshold: 0.1 }
    );

    if (footerRef.current) {
      observer.observe(footerRef.current);
    }

    return () => observer.disconnect();
  }, []);

  return (
    <footer ref={footerRef} className="relative bg-charcoal overflow-hidden">
      {/* Background decorations */}
      <div className="absolute inset-0 opacity-5">
        <div className="absolute inset-0 grid-pattern" />
      </div>
      <div className="absolute top-0 left-1/4 w-96 h-96 bg-rust-500/10 rounded-full blur-3xl" />
      <div className="absolute bottom-0 right-1/4 w-72 h-72 bg-amber-400/10 rounded-full blur-3xl" />

      <div className="relative mx-auto max-w-7xl px-6 py-16 lg:px-8 lg:py-20">
        <div className="xl:grid xl:grid-cols-3 xl:gap-8">
          {/* Brand section */}
          <div
            className={`space-y-6 transition-all duration-700 ${
              isVisible ? "opacity-100 translate-y-0" : "opacity-0 translate-y-8"
            }`}
          >
            <Link href="/" className="group inline-flex items-center gap-2">
              <span className="font-display text-2xl font-bold text-white transition-colors group-hover:text-rust-400">
                r<span className="text-rust-400 group-hover:text-rust-300">indexer</span>
              </span>
            </Link>
            <p className="text-sm leading-relaxed text-gray-400 max-w-xs">
              High-performance EVM blockchain indexing tool built in Rust.
              Open source under the MIT License.
            </p>
            <div className="flex items-center gap-4">
              <Link
                href="https://github.com/joshstevens19/rindexer"
                className="group flex items-center justify-center w-10 h-10 rounded-xl bg-white/5 text-gray-400 transition-all hover:bg-rust-500/20 hover:text-rust-400"
              >
                <svg
                  className="h-5 w-5 transition-transform group-hover:scale-110"
                  fill="currentColor"
                  viewBox="0 0 24 24"
                >
                  <path
                    fillRule="evenodd"
                    d="M12 2C6.477 2 2 6.484 2 12.017c0 4.425 2.865 8.18 6.839 9.504.5.092.682-.217.682-.483 0-.237-.008-.868-.013-1.703-2.782.605-3.369-1.343-3.369-1.343-.454-1.158-1.11-1.466-1.11-1.466-.908-.62.069-.608.069-.608 1.003.07 1.531 1.032 1.531 1.032.892 1.53 2.341 1.088 2.91.832.092-.647.35-1.088.636-1.338-2.22-.253-4.555-1.113-4.555-4.951 0-1.093.39-1.988 1.029-2.688-.103-.253-.446-1.272.098-2.65 0 0 .84-.27 2.75 1.026A9.564 9.564 0 0112 6.844c.85.004 1.705.115 2.504.337 1.909-1.296 2.747-1.027 2.747-1.027.546 1.379.202 2.398.1 2.651.64.7 1.028 1.595 1.028 2.688 0 3.848-2.339 4.695-4.566 4.943.359.309.678.92.678 1.855 0 1.338-.012 2.419-.012 2.747 0 .268.18.58.688.482A10.019 10.019 0 0022 12.017C22 6.484 17.522 2 12 2z"
                    clipRule="evenodd"
                  />
                </svg>
              </Link>
              <div className="h-6 w-px bg-gray-700" />
              <div className="flex items-center gap-2 text-sm text-gray-500">
                <span className="relative flex h-2 w-2">
                  <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-green-400 opacity-75" />
                  <span className="relative inline-flex rounded-full h-2 w-2 bg-green-500" />
                </span>
                All systems operational
              </div>
            </div>
          </div>

          {/* Navigation sections */}
          <div className="mt-16 grid grid-cols-2 gap-8 xl:col-span-2 xl:mt-0">
            <div className="md:grid md:grid-cols-2 md:gap-8">
              <div
                className={`transition-all duration-700 delay-100 ${
                  isVisible ? "opacity-100 translate-y-0" : "opacity-0 translate-y-8"
                }`}
              >
                <h3 className="text-sm font-semibold text-white">Product</h3>
                <ul role="list" className="mt-4 space-y-3">
                  {navigation.product.map((item) => (
                    <li key={item.name}>
                      <Link
                        href={item.href}
                        className="group text-sm text-gray-400 transition-colors hover:text-rust-400"
                      >
                        <span className="link-underline">{item.name}</span>
                      </Link>
                    </li>
                  ))}
                </ul>
              </div>
              <div
                className={`mt-10 md:mt-0 transition-all duration-700 delay-200 ${
                  isVisible ? "opacity-100 translate-y-0" : "opacity-0 translate-y-8"
                }`}
              >
                <h3 className="text-sm font-semibold text-white">Resources</h3>
                <ul role="list" className="mt-4 space-y-3">
                  {navigation.resources.map((item) => (
                    <li key={item.name}>
                      <Link
                        href={item.href}
                        className="group text-sm text-gray-400 transition-colors hover:text-rust-400"
                      >
                        <span className="link-underline">{item.name}</span>
                      </Link>
                    </li>
                  ))}
                </ul>
              </div>
            </div>
            <div className="md:grid md:grid-cols-2 md:gap-8">
              <div
                className={`transition-all duration-700 delay-300 ${
                  isVisible ? "opacity-100 translate-y-0" : "opacity-0 translate-y-8"
                }`}
              >
                <h3 className="text-sm font-semibold text-white">Deployment</h3>
                <ul role="list" className="mt-4 space-y-3">
                  {navigation.deployment.map((item) => (
                    <li key={item.name}>
                      <Link
                        href={item.href}
                        className="group text-sm text-gray-400 transition-colors hover:text-rust-400"
                      >
                        <span className="link-underline">{item.name}</span>
                      </Link>
                    </li>
                  ))}
                </ul>
              </div>
              <div
                className={`mt-10 md:mt-0 transition-all duration-700 delay-400 ${
                  isVisible ? "opacity-100 translate-y-0" : "opacity-0 translate-y-8"
                }`}
              >
                <h3 className="text-sm font-semibold text-white">Community</h3>
                <ul role="list" className="mt-4 space-y-3">
                  {navigation.community.map((item) => (
                    <li key={item.name}>
                      <Link
                        href={item.href}
                        className="group text-sm text-gray-400 transition-colors hover:text-rust-400"
                      >
                        <span className="link-underline">{item.name}</span>
                      </Link>
                    </li>
                  ))}
                </ul>
              </div>
            </div>
          </div>
        </div>

        {/* Bottom section */}
        <div
          className={`mt-16 border-t border-gray-800 pt-8 transition-all duration-700 delay-500 ${
            isVisible ? "opacity-100 translate-y-0" : "opacity-0 translate-y-4"
          }`}
        >
          <div className="flex flex-col items-center justify-between gap-4 md:flex-row">
            <p className="text-xs text-gray-500">
              &copy; {new Date().getFullYear()} rindexer. All rights reserved. MIT License.
            </p>
            <div className="flex items-center gap-6">
              <span className="text-xs text-gray-500">
                Built with{" "}
                <span className="text-rust-400">Rust</span>
                {" "}for maximum performance
              </span>
            </div>
          </div>
        </div>
      </div>
    </footer>
  );
}
