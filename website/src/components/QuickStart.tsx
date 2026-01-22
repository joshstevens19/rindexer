"use client";

import { useEffect, useRef, useState } from "react";

const steps = [
  {
    step: "01",
    name: "Install",
    description: "Install rindexer with a single command.",
    code: "curl -L https://rindexer.xyz/install.sh | bash",
    icon: (
      <svg className="w-5 h-5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
        <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M4 16v1a3 3 0 003 3h10a3 3 0 003-3v-1m-4-4l-4 4m0 0l-4-4m4 4V4" />
      </svg>
    ),
  },
  {
    step: "02",
    name: "Create Project",
    description: "Generate a new indexer project with the CLI.",
    code: "rindexer new my-indexer --type no-code",
    icon: (
      <svg className="w-5 h-5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
        <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 6v6m0 0v6m0-6h6m-6 0H6" />
      </svg>
    ),
  },
  {
    step: "03",
    name: "Start Indexing",
    description: "Run your indexer with GraphQL API enabled.",
    code: "rindexer start all",
    icon: (
      <svg className="w-5 h-5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
        <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M14.752 11.168l-3.197-2.132A1 1 0 0010 9.87v4.263a1 1 0 001.555.832l3.197-2.132a1 1 0 000-1.664z" />
        <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M21 12a9 9 0 11-18 0 9 9 0 0118 0z" />
      </svg>
    ),
  },
];

export function QuickStart() {
  const [isVisible, setIsVisible] = useState(false);
  const [copiedIndex, setCopiedIndex] = useState<number | null>(null);
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

  const copyCode = async (code: string, index: number) => {
    await navigator.clipboard.writeText(code);
    setCopiedIndex(index);
    setTimeout(() => setCopiedIndex(null), 2000);
  };

  return (
    <section ref={sectionRef} className="relative bg-cream py-24 sm:py-32 overflow-hidden">
      {/* Background */}
      <div className="absolute inset-0 dot-pattern opacity-40" />
      <div className="absolute bottom-0 left-0 right-0 h-1/2 bg-gradient-to-t from-white to-transparent" />

      <div className="relative mx-auto max-w-7xl px-6 lg:px-8">
        <div
          className={`mx-auto max-w-2xl text-center transition-all duration-700 ${
            isVisible ? "opacity-100 translate-y-0" : "opacity-0 translate-y-8"
          }`}
        >
          <div className="inline-flex items-center gap-2 rounded-full bg-white border-2 border-rust-100 px-4 py-1.5 text-sm font-medium text-rust-600 mb-4 shadow-sm">
            <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M13 10V3L4 14h7v7l9-11h-7z" />
            </svg>
            Quick start
          </div>
          <h2 className="font-display text-3xl font-bold tracking-tight text-charcoal sm:text-4xl lg:text-5xl">
            Up and running in{" "}
            <span className="gradient-text">3 steps</span>
          </h2>
          <p className="mt-6 text-lg text-gray-600 leading-relaxed">
            From zero to indexed data with a GraphQL API in just a few commands.
          </p>
        </div>

        <div className="mx-auto mt-16 max-w-4xl">
          <div className="grid gap-8 md:grid-cols-3">
            {steps.map((step, index) => (
              <div
                key={step.step}
                className={`group relative transition-all duration-500 ${
                  isVisible ? "opacity-100 translate-y-0" : "opacity-0 translate-y-8"
                }`}
                style={{ transitionDelay: `${index * 150}ms` }}
              >
                {/* Connector line */}
                {index < steps.length - 1 && (
                  <div className="hidden md:block absolute top-8 left-full w-full h-0.5 -translate-y-1/2 z-0">
                    <div
                      className={`h-full bg-gradient-to-r from-rust-300 to-rust-200 transition-all duration-1000 ${
                        isVisible ? "w-full" : "w-0"
                      }`}
                      style={{ transitionDelay: `${(index + 1) * 200}ms` }}
                    />
                  </div>
                )}

                {/* Step content */}
                <div className="relative z-10">
                  <div className="mb-4 flex items-center gap-4">
                    <div className="icon-bounce flex h-14 w-14 items-center justify-center rounded-2xl bg-gradient-to-br from-rust-400 to-rust-600 text-white font-display font-bold text-lg shadow-lg shadow-rust-500/25">
                      {step.step}
                    </div>
                    <div>
                      <h3 className="font-display text-lg font-semibold text-charcoal">
                        {step.name}
                      </h3>
                      <p className="text-sm text-gray-500">{step.description}</p>
                    </div>
                  </div>

                  {/* Code block */}
                  <div className="group/code relative">
                    <div className="absolute -inset-1 bg-gradient-to-r from-rust-400 to-rust-600 rounded-2xl blur opacity-0 group-hover/code:opacity-20 transition-opacity duration-300" />
                    <div className="terminal-window relative">
                      <div className="terminal-header">
                        <div className="terminal-dot red" />
                        <div className="terminal-dot yellow" />
                        <div className="terminal-dot green" />
                      </div>
                      <div className="flex items-center justify-between p-4 font-mono text-xs">
                        <div className="flex items-center gap-2 overflow-hidden">
                          <span className="text-rust-400 shrink-0">$</span>
                          <code className="text-gray-100 truncate">{step.code}</code>
                        </div>
                        <button
                          onClick={() => copyCode(step.code, index)}
                          className="shrink-0 ml-2 p-1.5 rounded-lg text-gray-500 hover:text-white hover:bg-white/10 transition-all"
                        >
                          {copiedIndex === index ? (
                            <svg className="w-4 h-4 text-green-400" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M5 13l4 4L19 7" />
                            </svg>
                          ) : (
                            <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M8 16H6a2 2 0 01-2-2V6a2 2 0 012-2h8a2 2 0 012 2v2m-6 12h8a2 2 0 002-2v-8a2 2 0 00-2-2h-8a2 2 0 00-2 2v8a2 2 0 002 2z" />
                            </svg>
                          )}
                        </button>
                      </div>
                    </div>
                  </div>
                </div>
              </div>
            ))}
          </div>
        </div>
      </div>
    </section>
  );
}
