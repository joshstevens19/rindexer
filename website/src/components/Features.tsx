"use client";

import { useEffect, useRef, useState } from "react";

const features = [
  {
    name: "No-Code Configuration",
    description:
      "Define your indexing logic with a simple YAML file. No programming required to get started.",
    icon: (
      <svg className="h-6 w-6" fill="none" viewBox="0 0 24 24" stroke="currentColor">
        <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 12h6m-6 4h6m2 5H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z" />
      </svg>
    ),
    gradient: "from-amber-400 to-orange-500",
  },
  {
    name: "Built for Speed",
    description:
      "Written in Rust for maximum performance. Handle millions of events with minimal resource usage.",
    icon: (
      <svg className="h-6 w-6" fill="none" viewBox="0 0 24 24" stroke="currentColor">
        <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M13 10V3L4 14h7v7l9-11h-7z" />
      </svg>
    ),
    gradient: "from-rust-400 to-rust-600",
  },
  {
    name: "Any EVM Chain",
    description:
      "Works with Ethereum, Polygon, Arbitrum, Optimism, Base, and any EVM-compatible chain.",
    icon: (
      <svg className="h-6 w-6" fill="none" viewBox="0 0 24 24" stroke="currentColor">
        <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M21 12a9 9 0 01-9 9m9-9a9 9 0 00-9-9m9 9H3m9 9a9 9 0 01-9-9m9 9c1.657 0 3-4.03 3-9s-1.343-9-3-9m0 18c-1.657 0-3-4.03-3-9s1.343-9 3-9m-9 9a9 9 0 019-9" />
      </svg>
    ),
    gradient: "from-purple-400 to-indigo-500",
  },
  {
    name: "Instant GraphQL API",
    description:
      "Automatically generates a GraphQL API for your indexed data. Query instantly with zero setup.",
    icon: (
      <svg className="h-6 w-6" fill="none" viewBox="0 0 24 24" stroke="currentColor">
        <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M4 7v10c0 2.21 3.582 4 8 4s8-1.79 8-4V7M4 7c0 2.21 3.582 4 8 4s8-1.79 8-4M4 7c0-2.21 3.582-4 8-4s8 1.79 8 4m0 5c0 2.21-3.582 4-8 4s-8-1.79-8-4" />
      </svg>
    ),
    gradient: "from-pink-400 to-rose-500",
  },
  {
    name: "Real-time Streams",
    description:
      "Stream events to Kafka, Redis, RabbitMQ, AWS SNS/SQS, webhooks, and more in real-time.",
    icon: (
      <svg className="h-6 w-6" fill="none" viewBox="0 0 24 24" stroke="currentColor">
        <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M8.111 16.404a5.5 5.5 0 017.778 0M12 20h.01m-7.08-7.071c3.904-3.905 10.236-3.905 14.141 0M1.394 9.393c5.857-5.857 15.355-5.857 21.213 0" />
      </svg>
    ),
    gradient: "from-cyan-400 to-teal-500",
  },
  {
    name: "Advanced Extensibility",
    description:
      "Use the Rust framework to build custom indexing logic when you need full control.",
    icon: (
      <svg className="h-6 w-6" fill="none" viewBox="0 0 24 24" stroke="currentColor">
        <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M10 20l4-16m4 4l4 4-4 4M6 16l-4-4 4-4" />
      </svg>
    ),
    gradient: "from-emerald-400 to-green-500",
  },
];

export function Features() {
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
    <section ref={sectionRef} className="relative bg-white py-24 sm:py-32 overflow-hidden">
      {/* Background decoration */}
      <div className="absolute inset-0 grid-pattern opacity-50" />
      <div className="absolute top-0 right-0 w-96 h-96 bg-rust-100/50 rounded-full blur-3xl -translate-y-1/2 translate-x-1/2" />
      <div className="absolute bottom-0 left-0 w-96 h-96 bg-amber-100/50 rounded-full blur-3xl translate-y-1/2 -translate-x-1/2" />

      <div className="relative mx-auto max-w-7xl px-6 lg:px-8">
        <div
          className={`mx-auto max-w-2xl text-center transition-all duration-700 ${
            isVisible ? "opacity-100 translate-y-0" : "opacity-0 translate-y-8"
          }`}
        >
          <div className="inline-flex items-center gap-2 rounded-full bg-rust-50 px-4 py-1.5 text-sm font-medium text-rust-600 mb-4">
            <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M13 10V3L4 14h7v7l9-11h-7z" />
            </svg>
            Everything you need
          </div>
          <h2 className="font-display text-3xl font-bold tracking-tight text-charcoal sm:text-4xl lg:text-5xl">
            Powerful indexing{" "}
            <span className="gradient-text">made simple</span>
          </h2>
          <p className="mt-6 text-lg text-gray-600 leading-relaxed">
            Whether you&apos;re building a hackathon project or enterprise
            infrastructure, rindexer has you covered.
          </p>
        </div>

        <div className="mx-auto mt-16 max-w-5xl">
          <dl className="grid grid-cols-1 gap-6 sm:grid-cols-2 lg:grid-cols-3">
            {features.map((feature, index) => (
              <div
                key={feature.name}
                className={`group card-hover relative rounded-2xl border-2 border-transparent bg-white p-8 shadow-sm transition-all duration-500 ${
                  isVisible ? "opacity-100 translate-y-0" : "opacity-0 translate-y-8"
                }`}
                style={{ transitionDelay: `${index * 100}ms` }}
              >
                {/* Gradient border on hover */}
                <div className={`absolute inset-0 rounded-2xl bg-gradient-to-br ${feature.gradient} opacity-0 group-hover:opacity-100 transition-opacity duration-300`} style={{ padding: "2px" }}>
                  <div className="h-full w-full rounded-2xl bg-white" />
                </div>

                {/* Content */}
                <div className="relative">
                  <dt className="flex items-center gap-4">
                    <div className={`icon-bounce flex h-12 w-12 items-center justify-center rounded-xl bg-gradient-to-br ${feature.gradient} text-white shadow-lg`}>
                      {feature.icon}
                    </div>
                    <span className="font-display font-semibold text-charcoal text-lg">
                      {feature.name}
                    </span>
                  </dt>
                  <dd className="mt-4 text-sm leading-relaxed text-gray-600">
                    {feature.description}
                  </dd>
                </div>

                {/* Decorative corner */}
                <div className="absolute top-4 right-4 w-8 h-8 opacity-0 group-hover:opacity-100 transition-opacity duration-300">
                  <svg className={`w-full h-full text-rust-200`} viewBox="0 0 24 24" fill="currentColor">
                    <path d="M12 2L2 22h20L12 2z" opacity="0.2" />
                  </svg>
                </div>
              </div>
            ))}
          </dl>
        </div>
      </div>
    </section>
  );
}
