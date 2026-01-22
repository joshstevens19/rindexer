"use client";

import { useEffect, useRef, useState } from "react";
import { Player, PlayerRef } from "@remotion/player";
import { RindexerPromo } from "@/remotion/RindexerPromo";

export function VideoSection() {
  const [isVisible, setIsVisible] = useState(false);
  const [isLoaded, setIsLoaded] = useState(false);
  const sectionRef = useRef<HTMLElement>(null);
  const playerRef = useRef<PlayerRef>(null);

  useEffect(() => {
    const observer = new IntersectionObserver(
      ([entry]) => {
        if (entry.isIntersecting) {
          setIsVisible(true);
          setIsLoaded(true);
        }
      },
      { threshold: 0.2 }
    );

    if (sectionRef.current) {
      observer.observe(sectionRef.current);
    }

    return () => observer.disconnect();
  }, []);

  return (
    <section
      ref={sectionRef}
      className="relative bg-charcoal py-24 sm:py-32 overflow-hidden"
    >
      {/* Background decorations */}
      <div className="absolute inset-0 grid-pattern opacity-5" />
      <div className="absolute top-0 left-1/4 w-96 h-96 bg-rust-500/10 rounded-full blur-3xl" />
      <div className="absolute bottom-0 right-1/4 w-72 h-72 bg-amber-400/10 rounded-full blur-3xl" />

      <div className="relative mx-auto max-w-7xl px-6 lg:px-8">
        {/* Header */}
        <div
          className={`mx-auto max-w-2xl text-center mb-16 transition-all duration-700 ${
            isVisible ? "opacity-100 translate-y-0" : "opacity-0 translate-y-8"
          }`}
        >
          <div className="inline-flex items-center gap-2 rounded-full bg-rust-500/10 border border-rust-500/20 px-4 py-1.5 text-sm font-medium text-rust-400 mb-4">
            <svg
              className="w-4 h-4"
              fill="none"
              viewBox="0 0 24 24"
              stroke="currentColor"
            >
              <path
                strokeLinecap="round"
                strokeLinejoin="round"
                strokeWidth={2}
                d="M14.752 11.168l-3.197-2.132A1 1 0 0010 9.87v4.263a1 1 0 001.555.832l3.197-2.132a1 1 0 000-1.664z"
              />
              <path
                strokeLinecap="round"
                strokeLinejoin="round"
                strokeWidth={2}
                d="M21 12a9 9 0 11-18 0 9 9 0 0118 0z"
              />
            </svg>
            Watch the overview
          </div>
          <h2 className="font-display text-3xl font-bold tracking-tight text-white sm:text-4xl lg:text-5xl">
            See rindexer{" "}
            <span className="gradient-text">in action</span>
          </h2>
          <p className="mt-6 text-lg text-gray-400 leading-relaxed">
            Discover how rindexer makes blockchain indexing simple, fast, and
            powerful in this quick overview.
          </p>
        </div>

        {/* Video Player */}
        <div
          className={`mx-auto max-w-4xl transition-all duration-700 delay-200 ${
            isVisible ? "opacity-100 translate-y-0 scale-100" : "opacity-0 translate-y-8 scale-95"
          }`}
        >
          <div className="group relative">
            {/* Glow effect */}
            <div className="absolute -inset-1 bg-gradient-to-r from-rust-500 via-rust-400 to-amber-400 rounded-3xl blur-xl opacity-30 group-hover:opacity-40 transition-opacity duration-500" />

            {/* Video container */}
            <div className="relative overflow-hidden rounded-2xl border-2 border-white/10 bg-midnight shadow-2xl">
              {/* Video header bar */}
              <div className="flex items-center justify-between border-b border-white/10 bg-gradient-to-b from-white/10 to-white/5 px-4 py-3">
                <div className="flex items-center gap-2">
                  <div className="flex gap-2">
                    <div className="w-3 h-3 rounded-full bg-[#ff5f56]" />
                    <div className="w-3 h-3 rounded-full bg-[#ffbd2e]" />
                    <div className="w-3 h-3 rounded-full bg-[#27c93f]" />
                  </div>
                  <span className="ml-3 text-xs text-gray-500 font-mono">
                    rindexer-overview.mp4
                  </span>
                </div>
                <div className="flex items-center gap-3">
                  <span className="text-xs text-gray-500">15 seconds</span>
                </div>
              </div>

              {/* Player area */}
              <div className="relative aspect-video bg-midnight">
                {isLoaded ? (
                  <Player
                    ref={playerRef}
                    component={RindexerPromo}
                    durationInFrames={450}
                    fps={30}
                    compositionWidth={1280}
                    compositionHeight={720}
                    style={{
                      width: "100%",
                      height: "100%",
                    }}
                    controls
                    autoPlay={false}
                    loop
                    clickToPlay
                  />
                ) : (
                  <div className="absolute inset-0 flex items-center justify-center">
                    <div className="w-16 h-16 rounded-full bg-rust-500/20 flex items-center justify-center">
                      <svg
                        className="w-8 h-8 text-rust-400 animate-pulse"
                        fill="none"
                        viewBox="0 0 24 24"
                        stroke="currentColor"
                      >
                        <path
                          strokeLinecap="round"
                          strokeLinejoin="round"
                          strokeWidth={2}
                          d="M14.752 11.168l-3.197-2.132A1 1 0 0010 9.87v4.263a1 1 0 001.555.832l3.197-2.132a1 1 0 000-1.664z"
                        />
                      </svg>
                    </div>
                  </div>
                )}
              </div>
            </div>
          </div>

          {/* Video features */}
          <div
            className={`mt-8 grid grid-cols-3 gap-6 transition-all duration-700 delay-400 ${
              isVisible ? "opacity-100 translate-y-0" : "opacity-0 translate-y-4"
            }`}
          >
            {[
              { icon: "⚡", label: "Built in Rust", desc: "Maximum performance" },
              { icon: "📄", label: "No-Code Config", desc: "YAML simplicity" },
              { icon: "🔗", label: "Any EVM Chain", desc: "Universal support" },
            ].map((item) => (
              <div
                key={item.label}
                className="group flex flex-col items-center text-center p-4 rounded-xl bg-white/5 border border-white/10 transition-all hover:bg-white/10 hover:border-rust-500/30"
              >
                <span className="text-2xl mb-2 group-hover:scale-110 transition-transform">
                  {item.icon}
                </span>
                <span className="text-sm font-semibold text-white">
                  {item.label}
                </span>
                <span className="text-xs text-gray-500 mt-1">{item.desc}</span>
              </div>
            ))}
          </div>
        </div>
      </div>
    </section>
  );
}
