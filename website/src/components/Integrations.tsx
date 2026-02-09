"use client";

import { useEffect, useRef, useState } from "react";

const integrations = [
  { name: "PostgreSQL", category: "Storage", color: "from-blue-400 to-blue-600" },
  { name: "ClickHouse", category: "Storage", color: "from-yellow-400 to-amber-500" },
  { name: "GraphQL", category: "API", color: "from-pink-400 to-pink-600" },
  { name: "Kafka", category: "Streaming", color: "from-gray-600 to-gray-800" },
  { name: "Redis", category: "Streaming", color: "from-red-400 to-red-600" },
  { name: "RabbitMQ", category: "Streaming", color: "from-orange-400 to-orange-600" },
  { name: "AWS SNS", category: "Streaming", color: "from-amber-400 to-amber-600" },
  { name: "AWS SQS", category: "Streaming", color: "from-amber-500 to-amber-700" },
  { name: "Webhook", category: "Streaming", color: "from-purple-400 to-purple-600" },
  { name: "Telegram", category: "Notifications", color: "from-sky-400 to-sky-600" },
  { name: "Discord", category: "Notifications", color: "from-indigo-400 to-indigo-600" },
  { name: "Slack", category: "Notifications", color: "from-emerald-400 to-emerald-600" },
  { name: "Docker", category: "Deployment", color: "from-blue-500 to-blue-700" },
  { name: "Kubernetes", category: "Deployment", color: "from-blue-400 to-indigo-600" },
];

export function Integrations() {
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
      {/* Background */}
      <div className="absolute inset-0 grid-pattern opacity-30" />
      <div className="absolute top-0 left-1/2 -translate-x-1/2 w-[1000px] h-[500px] bg-gradient-radial from-rust-50/50 to-transparent" />

      <div className="relative mx-auto max-w-7xl px-6 lg:px-8">
        <div
          className={`mx-auto max-w-2xl text-center transition-all duration-700 ${
            isVisible ? "opacity-100 translate-y-0" : "opacity-0 translate-y-8"
          }`}
        >
          <div className="inline-flex items-center gap-2 rounded-full bg-rust-50 px-4 py-1.5 text-sm font-medium text-rust-600 mb-4">
            <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M11 4a2 2 0 114 0v1a1 1 0 001 1h3a1 1 0 011 1v3a1 1 0 01-1 1h-1a2 2 0 100 4h1a1 1 0 011 1v3a1 1 0 01-1 1h-3a1 1 0 01-1-1v-1a2 2 0 10-4 0v1a1 1 0 01-1 1H7a1 1 0 01-1-1v-3a1 1 0 00-1-1H4a2 2 0 110-4h1a1 1 0 001-1V7a1 1 0 011-1h3a1 1 0 001-1V4z" />
            </svg>
            Integrations
          </div>
          <h2 className="font-display text-3xl font-bold tracking-tight text-charcoal sm:text-4xl lg:text-5xl">
            Works with{" "}
            <span className="gradient-text">your stack</span>
          </h2>
          <p className="mt-6 text-lg text-gray-600 leading-relaxed">
            Store data in PostgreSQL, stream to message queues, send
            notifications, and deploy anywhere.
          </p>
        </div>

        <div className="mx-auto mt-16 max-w-5xl">
          <div className="grid grid-cols-3 gap-4 sm:grid-cols-4 md:grid-cols-5 lg:grid-cols-7">
            {integrations.map((integration, index) => (
              <div
                key={integration.name}
                className={`group card-hover relative flex flex-col items-center justify-center gap-3 rounded-2xl border-2 border-gray-100 bg-white p-4 transition-all duration-500 ${
                  isVisible ? "opacity-100 translate-y-0 scale-100" : "opacity-0 translate-y-4 scale-95"
                }`}
                style={{ transitionDelay: `${index * 50}ms` }}
              >
                {/* Icon container with gradient */}
                <div className={`icon-bounce flex h-12 w-12 items-center justify-center rounded-xl bg-gradient-to-br ${integration.color} text-white text-sm font-bold shadow-md`}>
                  {integration.name.slice(0, 2).toUpperCase()}
                </div>

                {/* Name */}
                <span className="text-center text-xs font-medium text-gray-700 group-hover:text-rust-600 transition-colors">
                  {integration.name}
                </span>

                {/* Category badge on hover */}
                <div className="absolute -top-2 left-1/2 -translate-x-1/2 opacity-0 group-hover:opacity-100 scale-90 group-hover:scale-100 transition-all duration-300">
                  <span className="inline-block rounded-full bg-charcoal px-2 py-0.5 text-[10px] font-medium text-white whitespace-nowrap">
                    {integration.category}
                  </span>
                </div>

                {/* Hover ring */}
                <div className={`absolute inset-0 rounded-2xl opacity-0 group-hover:opacity-100 transition-opacity duration-300 bg-gradient-to-br ${integration.color} p-[2px]`}>
                  <div className="h-full w-full rounded-2xl bg-white" />
                </div>
              </div>
            ))}
          </div>
        </div>

        {/* Bottom decoration */}
        <div
          className={`mt-12 flex justify-center transition-all duration-700 delay-500 ${
            isVisible ? "opacity-100 translate-y-0" : "opacity-0 translate-y-4"
          }`}
        >
          <div className="inline-flex items-center gap-2 text-sm text-gray-500">
            <span className="flex h-2 w-2">
              <span className="animate-ping absolute inline-flex h-2 w-2 rounded-full bg-rust-400 opacity-75" />
              <span className="relative inline-flex rounded-full h-2 w-2 bg-rust-500" />
            </span>
            And more integrations coming soon...
          </div>
        </div>
      </div>
    </section>
  );
}
