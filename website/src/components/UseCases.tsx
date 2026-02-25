"use client";

import { useEffect, useRef, useState } from "react";

const useCases = [
  {
    name: "Hackathons",
    description:
      "Spin up a quick indexer with GraphQL API in minutes. No backend code needed.",
    icon: (
      <svg className="h-6 w-6" fill="none" viewBox="0 0 24 24" stroke="currentColor">
        <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 8v4l3 3m6-3a9 9 0 11-18 0 9 9 0 0118 0z" />
      </svg>
    ),
    gradient: "from-amber-400 to-orange-500",
    emoji: "🚀",
  },
  {
    name: "Data Reporting",
    description:
      "Build comprehensive analytics dashboards with historical blockchain data.",
    icon: (
      <svg className="h-6 w-6" fill="none" viewBox="0 0 24 24" stroke="currentColor">
        <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 19v-6a2 2 0 00-2-2H5a2 2 0 00-2 2v6a2 2 0 002 2h2a2 2 0 002-2zm0 0V9a2 2 0 012-2h2a2 2 0 012 2v10m-6 0a2 2 0 002 2h2a2 2 0 002-2m0 0V5a2 2 0 012-2h2a2 2 0 012 2v14a2 2 0 01-2 2h-2a2 2 0 01-2-2z" />
      </svg>
    ),
    gradient: "from-blue-400 to-indigo-500",
    emoji: "📊",
  },
  {
    name: "dApp Development",
    description:
      "Power your decentralized applications with real-time indexed data.",
    icon: (
      <svg className="h-6 w-6" fill="none" viewBox="0 0 24 24" stroke="currentColor">
        <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9.75 17L9 20l-1 1h8l-1-1-.75-3M3 13h18M5 17h14a2 2 0 002-2V5a2 2 0 00-2-2H5a2 2 0 00-2 2v10a2 2 0 002 2z" />
      </svg>
    ),
    gradient: "from-purple-400 to-violet-500",
    emoji: "🌐",
  },
  {
    name: "Enterprise",
    description:
      "Production-grade indexing infrastructure with streaming and PostgreSQL.",
    icon: (
      <svg className="h-6 w-6" fill="none" viewBox="0 0 24 24" stroke="currentColor">
        <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 21V5a2 2 0 00-2-2H7a2 2 0 00-2 2v16m14 0h2m-2 0h-5m-9 0H3m2 0h5M9 7h1m-1 4h1m4-4h1m-1 4h1m-5 10v-5a1 1 0 011-1h2a1 1 0 011 1v5m-4 0h4" />
      </svg>
    ),
    gradient: "from-emerald-400 to-teal-500",
    emoji: "🏢",
  },
  {
    name: "Prototyping",
    description:
      "Rapidly test ideas and validate concepts before building full solutions.",
    icon: (
      <svg className="h-6 w-6" fill="none" viewBox="0 0 24 24" stroke="currentColor">
        <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9.663 17h4.673M12 3v1m6.364 1.636l-.707.707M21 12h-1M4 12H3m3.343-5.657l-.707-.707m2.828 9.9a5 5 0 117.072 0l-.548.547A3.374 3.374 0 0014 18.469V19a2 2 0 11-4 0v-.531c0-.895-.356-1.754-.988-2.386l-.548-.547z" />
      </svg>
    ),
    gradient: "from-pink-400 to-rose-500",
    emoji: "💡",
  },
];

export function UseCases() {
  const [isVisible, setIsVisible] = useState(false);
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

  return (
    <section ref={sectionRef} className="relative bg-cream py-24 sm:py-32 overflow-hidden">
      {/* Background elements */}
      <div className="absolute inset-0 dot-pattern opacity-40" />
      <div className="absolute top-20 right-20 w-64 h-64 bg-rust-200/30 rounded-full blur-3xl animate-float" />
      <div className="absolute bottom-20 left-20 w-72 h-72 bg-amber-200/30 rounded-full blur-3xl animate-float-delayed" />

      <div className="relative mx-auto max-w-7xl px-6 lg:px-8">
        <div
          className={`mx-auto max-w-2xl text-center transition-all duration-700 ${
            isVisible ? "opacity-100 translate-y-0" : "opacity-0 translate-y-8"
          }`}
        >
          <div className="inline-flex items-center gap-2 rounded-full bg-white border-2 border-rust-100 px-4 py-1.5 text-sm font-medium text-rust-600 mb-4 shadow-sm">
            <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M4 6a2 2 0 012-2h2a2 2 0 012 2v2a2 2 0 01-2 2H6a2 2 0 01-2-2V6zM14 6a2 2 0 012-2h2a2 2 0 012 2v2a2 2 0 01-2 2h-2a2 2 0 01-2-2V6zM4 16a2 2 0 012-2h2a2 2 0 012 2v2a2 2 0 01-2 2H6a2 2 0 01-2-2v-2zM14 16a2 2 0 012-2h2a2 2 0 012 2v2a2 2 0 01-2 2h-2a2 2 0 01-2-2v-2z" />
            </svg>
            Use cases
          </div>
          <h2 className="font-display text-3xl font-bold tracking-tight text-charcoal sm:text-4xl lg:text-5xl">
            Built for{" "}
            <span className="gradient-text">every scenario</span>
          </h2>
          <p className="mt-6 text-lg text-gray-600 leading-relaxed">
            From weekend hackathons to enterprise deployments, rindexer scales
            with your needs.
          </p>
        </div>

        <div className="mx-auto mt-16 max-w-5xl">
          <div className="grid grid-cols-1 gap-6 sm:grid-cols-2 lg:grid-cols-3">
            {useCases.map((useCase, index) => (
              <div
                key={useCase.name}
                className={`group card-hover relative overflow-hidden rounded-2xl border-2 border-white bg-white p-6 shadow-sm transition-all duration-500 ${
                  isVisible ? "opacity-100 translate-y-0" : "opacity-0 translate-y-8"
                }`}
                style={{ transitionDelay: `${index * 100}ms` }}
              >
                {/* Gradient background on hover */}
                <div className={`absolute inset-0 bg-gradient-to-br ${useCase.gradient} opacity-0 group-hover:opacity-5 transition-opacity duration-300`} />

                {/* Content */}
                <div className="relative flex items-start gap-4">
                  <div className={`icon-bounce flex h-12 w-12 shrink-0 items-center justify-center rounded-xl bg-gradient-to-br ${useCase.gradient} text-white shadow-lg`}>
                    {useCase.icon}
                  </div>
                  <div>
                    <h3 className="font-display font-semibold text-charcoal text-lg flex items-center gap-2">
                      {useCase.name}
                      <span className="text-base opacity-0 group-hover:opacity-100 transition-opacity duration-300">
                        {useCase.emoji}
                      </span>
                    </h3>
                    <p className="mt-2 text-sm text-gray-600 leading-relaxed">
                      {useCase.description}
                    </p>
                  </div>
                </div>

                {/* Corner decoration */}
                <div className="absolute -bottom-2 -right-2 w-16 h-16 opacity-0 group-hover:opacity-100 transition-opacity duration-300">
                  <div className={`w-full h-full bg-gradient-to-br ${useCase.gradient} opacity-10 rounded-tl-3xl`} />
                </div>
              </div>
            ))}
          </div>
        </div>
      </div>
    </section>
  );
}
