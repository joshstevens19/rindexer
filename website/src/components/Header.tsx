"use client";

import { useState, useEffect } from "react";
import Link from "next/link";

export function Header() {
  const [mobileMenuOpen, setMobileMenuOpen] = useState(false);
  const [scrolled, setScrolled] = useState(false);

  useEffect(() => {
    const handleScroll = () => {
      setScrolled(window.scrollY > 20);
    };
    window.addEventListener("scroll", handleScroll);
    return () => window.removeEventListener("scroll", handleScroll);
  }, []);

  return (
    <header
      className={`sticky top-0 z-50 transition-all duration-300 ${
        scrolled
          ? "bg-white/90 backdrop-blur-xl shadow-sm border-b border-rust-100/50"
          : "bg-transparent"
      }`}
    >
      <nav className="mx-auto flex max-w-7xl items-center justify-between px-6 py-4 lg:px-8">
        <div className="flex lg:flex-1">
          <Link href="/" className="group -m-1.5 p-1.5 flex items-center gap-2">
            {/* Animated logo */}
            <div className="relative">
              <span className="font-display text-xl font-bold text-charcoal transition-colors group-hover:text-rust-600">
                r<span className="text-rust-500 group-hover:text-rust-600">indexer</span>
              </span>
              {/* Playful underline on hover */}
              <div className="absolute -bottom-1 left-0 h-0.5 w-0 bg-gradient-to-r from-rust-400 to-rust-600 transition-all duration-300 group-hover:w-full rounded-full" />
            </div>
          </Link>
        </div>

        {/* Mobile menu button */}
        <div className="flex lg:hidden">
          <button
            type="button"
            onClick={() => setMobileMenuOpen(!mobileMenuOpen)}
            className="relative -m-2.5 inline-flex items-center justify-center rounded-xl p-2.5 text-charcoal transition-colors hover:bg-rust-50"
          >
            <span className="sr-only">Open main menu</span>
            <div className="relative w-6 h-6">
              <span
                className={`absolute left-0 w-6 h-0.5 bg-current transform transition-all duration-300 ${
                  mobileMenuOpen ? "top-3 rotate-45" : "top-1.5"
                }`}
              />
              <span
                className={`absolute left-0 top-3 w-6 h-0.5 bg-current transition-all duration-300 ${
                  mobileMenuOpen ? "opacity-0 scale-0" : "opacity-100"
                }`}
              />
              <span
                className={`absolute left-0 w-6 h-0.5 bg-current transform transition-all duration-300 ${
                  mobileMenuOpen ? "top-3 -rotate-45" : "top-[18px]"
                }`}
              />
            </div>
          </button>
        </div>

        {/* Desktop navigation */}
        <div className="hidden lg:flex lg:gap-x-1">
          {[
            { href: "https://rindexer.xyz/docs/introduction/installation", label: "Documentation" },
            { href: "https://rindexer.xyz/docs/start-building/no-code", label: "Quick Start" },
            { href: "https://github.com/joshstevens19/rindexer/tree/master/examples", label: "Examples" },
            { href: "https://rindexer.xyz/docs/changelog", label: "Changelog" },
          ].map((link) => (
            <Link
              key={link.label}
              href={link.href}
              className="relative px-4 py-2 text-sm font-medium text-gray-600 transition-colors hover:text-rust-600 group"
            >
              {link.label}
              {/* Animated dot indicator */}
              <span className="absolute bottom-1 left-1/2 -translate-x-1/2 w-1 h-1 rounded-full bg-rust-500 scale-0 group-hover:scale-100 transition-transform duration-200" />
            </Link>
          ))}
        </div>

        <div className="hidden lg:flex lg:flex-1 lg:justify-end lg:gap-x-3">
          <Link
            href="https://github.com/joshstevens19/rindexer"
            className="group flex items-center gap-2 px-3 py-2 text-sm font-medium text-gray-600 transition-colors hover:text-rust-600"
          >
            <svg
              className="h-5 w-5 transition-transform duration-300 group-hover:rotate-12"
              fill="currentColor"
              viewBox="0 0 24 24"
            >
              <path
                fillRule="evenodd"
                d="M12 2C6.477 2 2 6.484 2 12.017c0 4.425 2.865 8.18 6.839 9.504.5.092.682-.217.682-.483 0-.237-.008-.868-.013-1.703-2.782.605-3.369-1.343-3.369-1.343-.454-1.158-1.11-1.466-1.11-1.466-.908-.62.069-.608.069-.608 1.003.07 1.531 1.032 1.531 1.032.892 1.53 2.341 1.088 2.91.832.092-.647.35-1.088.636-1.338-2.22-.253-4.555-1.113-4.555-4.951 0-1.093.39-1.988 1.029-2.688-.103-.253-.446-1.272.098-2.65 0 0 .84-.27 2.75 1.026A9.564 9.564 0 0112 6.844c.85.004 1.705.115 2.504.337 1.909-1.296 2.747-1.027 2.747-1.027.546 1.379.202 2.398.1 2.651.64.7 1.028 1.595 1.028 2.688 0 3.848-2.339 4.695-4.566 4.943.359.309.678.92.678 1.855 0 1.338-.012 2.419-.012 2.747 0 .268.18.58.688.482A10.019 10.019 0 0022 12.017C22 6.484 17.522 2 12 2z"
                clipRule="evenodd"
              />
            </svg>
            <span className="hidden xl:inline">GitHub</span>
          </Link>
          <Link
            href="https://rindexer.xyz/docs/introduction/installation"
            className="magnetic-btn group relative overflow-hidden rounded-xl bg-gradient-to-r from-rust-500 to-rust-600 px-5 py-2.5 text-sm font-semibold text-white shadow-md shadow-rust-500/20 transition-all hover:shadow-lg hover:shadow-rust-500/30"
          >
            <span className="relative z-10 flex items-center gap-1.5">
              Get Started
              <svg
                className="w-4 h-4 transition-transform group-hover:translate-x-0.5"
                fill="none"
                viewBox="0 0 24 24"
                stroke="currentColor"
              >
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M13 7l5 5m0 0l-5 5m5-5H6" />
              </svg>
            </span>
            {/* Shimmer */}
            <div className="absolute inset-0 -translate-x-full group-hover:translate-x-full transition-transform duration-500 bg-gradient-to-r from-transparent via-white/20 to-transparent" />
          </Link>
        </div>
      </nav>

      {/* Mobile menu */}
      <div
        className={`lg:hidden overflow-hidden transition-all duration-300 ease-out ${
          mobileMenuOpen ? "max-h-96 opacity-100" : "max-h-0 opacity-0"
        }`}
      >
        <div className="space-y-1 border-t border-rust-100 bg-white/95 backdrop-blur-xl px-6 py-4">
          {[
            { href: "https://rindexer.xyz/docs/introduction/installation", label: "Documentation" },
            { href: "https://rindexer.xyz/docs/start-building/no-code", label: "Quick Start" },
            { href: "https://github.com/joshstevens19/rindexer/tree/master/examples", label: "Examples" },
            { href: "https://rindexer.xyz/docs/changelog", label: "Changelog" },
          ].map((link, index) => (
            <Link
              key={link.label}
              href={link.href}
              className="block rounded-xl px-4 py-3 text-base font-medium text-gray-600 transition-all hover:bg-rust-50 hover:text-rust-600 hover:translate-x-1"
              onClick={() => setMobileMenuOpen(false)}
              style={{ animationDelay: `${index * 50}ms` }}
            >
              {link.label}
            </Link>
          ))}
          <div className="mt-4 flex flex-col gap-3 border-t border-rust-100 pt-4">
            <Link
              href="https://github.com/joshstevens19/rindexer"
              className="flex items-center gap-2 rounded-xl px-4 py-3 text-base font-medium text-gray-600 transition-all hover:bg-rust-50 hover:text-rust-600"
              onClick={() => setMobileMenuOpen(false)}
            >
              <svg className="h-5 w-5" fill="currentColor" viewBox="0 0 24 24">
                <path
                  fillRule="evenodd"
                  d="M12 2C6.477 2 2 6.484 2 12.017c0 4.425 2.865 8.18 6.839 9.504.5.092.682-.217.682-.483 0-.237-.008-.868-.013-1.703-2.782.605-3.369-1.343-3.369-1.343-.454-1.158-1.11-1.466-1.11-1.466-.908-.62.069-.608.069-.608 1.003.07 1.531 1.032 1.531 1.032.892 1.53 2.341 1.088 2.91.832.092-.647.35-1.088.636-1.338-2.22-.253-4.555-1.113-4.555-4.951 0-1.093.39-1.988 1.029-2.688-.103-.253-.446-1.272.098-2.65 0 0 .84-.27 2.75 1.026A9.564 9.564 0 0112 6.844c.85.004 1.705.115 2.504.337 1.909-1.296 2.747-1.027 2.747-1.027.546 1.379.202 2.398.1 2.651.64.7 1.028 1.595 1.028 2.688 0 3.848-2.339 4.695-4.566 4.943.359.309.678.92.678 1.855 0 1.338-.012 2.419-.012 2.747 0 .268.18.58.688.482A10.019 10.019 0 0022 12.017C22 6.484 17.522 2 12 2z"
                  clipRule="evenodd"
                />
              </svg>
              GitHub
            </Link>
            <Link
              href="https://rindexer.xyz/docs/introduction/installation"
              className="rounded-xl bg-gradient-to-r from-rust-500 to-rust-600 px-4 py-3 text-center text-base font-semibold text-white shadow-md transition-all hover:shadow-lg active:scale-[0.98]"
              onClick={() => setMobileMenuOpen(false)}
            >
              Get Started
            </Link>
          </div>
        </div>
      </div>
    </header>
  );
}
