import {
  AbsoluteFill,
  interpolate,
  useCurrentFrame,
  useVideoConfig,
  spring,
  Sequence,
  Easing,
} from "remotion";
import React from "react";

// Color palette
const colors = {
  rust500: "#f97316",
  rust600: "#ea580c",
  rust400: "#fb923c",
  amber400: "#ffc107",
  charcoal: "#1a1a2e",
  midnight: "#0f0f1a",
  cream: "#fffbf5",
  white: "#ffffff",
};

// Animated background with floating particles
const AnimatedBackground: React.FC<{ dark?: boolean }> = ({ dark = true }) => {
  const frame = useCurrentFrame();
  const bgColor = dark ? colors.midnight : colors.cream;

  return (
    <AbsoluteFill style={{ backgroundColor: bgColor }}>
      {/* Grid pattern */}
      <div
        style={{
          position: "absolute",
          inset: 0,
          backgroundImage: `linear-gradient(${dark ? "rgba(249,115,22,0.03)" : "rgba(249,115,22,0.05)"} 1px, transparent 1px), linear-gradient(90deg, ${dark ? "rgba(249,115,22,0.03)" : "rgba(249,115,22,0.05)"} 1px, transparent 1px)`,
          backgroundSize: "40px 40px",
        }}
      />

      {/* Floating orbs */}
      {[...Array(5)].map((_, i) => {
        const offset = i * 72;
        const x = interpolate(
          frame + offset,
          [0, 300],
          [100 + i * 200, 200 + i * 200],
          { extrapolateRight: "clamp" }
        );
        const y = interpolate(
          Math.sin((frame + offset) / 50) * 100,
          [-100, 100],
          [100 + i * 100, 200 + i * 80]
        );
        return (
          <div
            key={i}
            style={{
              position: "absolute",
              left: x,
              top: y,
              width: 200 + i * 50,
              height: 200 + i * 50,
              borderRadius: "50%",
              background: `radial-gradient(circle, ${i % 2 === 0 ? colors.rust500 : colors.amber400}20, transparent)`,
              filter: "blur(60px)",
            }}
          />
        );
      })}
    </AbsoluteFill>
  );
};

// Logo animation
const Logo: React.FC = () => {
  const frame = useCurrentFrame();
  const { fps } = useVideoConfig();

  const scale = spring({
    frame,
    fps,
    config: { damping: 12, stiffness: 100 },
  });

  const opacity = interpolate(frame, [0, 15], [0, 1], {
    extrapolateRight: "clamp",
  });

  return (
    <div
      style={{
        display: "flex",
        alignItems: "center",
        justifyContent: "center",
        transform: `scale(${scale})`,
        opacity,
      }}
    >
      <span
        style={{
          fontFamily: "Sora, sans-serif",
          fontSize: 72,
          fontWeight: 800,
          color: colors.white,
        }}
      >
        r
        <span style={{ color: colors.rust500 }}>indexer</span>
      </span>
    </div>
  );
};

// Tagline with typing effect
const Tagline: React.FC<{ text: string; startFrame?: number }> = ({
  text,
  startFrame = 0,
}) => {
  const frame = useCurrentFrame();
  const localFrame = frame - startFrame;

  if (localFrame < 0) return null;

  const opacity = interpolate(localFrame, [0, 10], [0, 1], {
    extrapolateRight: "clamp",
  });

  const translateY = interpolate(localFrame, [0, 20], [30, 0], {
    extrapolateRight: "clamp",
    easing: Easing.out(Easing.cubic),
  });

  return (
    <div
      style={{
        opacity,
        transform: `translateY(${translateY}px)`,
        textAlign: "center",
      }}
    >
      <span
        style={{
          fontFamily: "DM Sans, sans-serif",
          fontSize: 28,
          color: colors.rust400,
          letterSpacing: "0.05em",
        }}
      >
        {text}
      </span>
    </div>
  );
};

// Feature card component
const FeatureCard: React.FC<{
  title: string;
  description: string;
  icon: string;
  index: number;
  startFrame: number;
}> = ({ title, description, icon, index, startFrame }) => {
  const frame = useCurrentFrame();
  const { fps } = useVideoConfig();
  const localFrame = frame - startFrame;

  if (localFrame < 0) return null;

  const scale = spring({
    frame: localFrame,
    fps,
    config: { damping: 12, stiffness: 80 },
  });

  const opacity = interpolate(localFrame, [0, 15], [0, 1], {
    extrapolateRight: "clamp",
  });

  const translateX = interpolate(localFrame, [0, 20], [index % 2 === 0 ? -100 : 100, 0], {
    extrapolateRight: "clamp",
    easing: Easing.out(Easing.cubic),
  });

  return (
    <div
      style={{
        display: "flex",
        flexDirection: "column",
        alignItems: "center",
        padding: 30,
        background: `linear-gradient(135deg, ${colors.charcoal}, ${colors.midnight})`,
        borderRadius: 20,
        border: `2px solid ${colors.rust500}30`,
        width: 300,
        opacity,
        transform: `scale(${scale}) translateX(${translateX}px)`,
      }}
    >
      <div
        style={{
          fontSize: 48,
          marginBottom: 16,
        }}
      >
        {icon}
      </div>
      <h3
        style={{
          fontFamily: "Sora, sans-serif",
          fontSize: 22,
          fontWeight: 700,
          color: colors.white,
          marginBottom: 8,
          textAlign: "center",
        }}
      >
        {title}
      </h3>
      <p
        style={{
          fontFamily: "DM Sans, sans-serif",
          fontSize: 14,
          color: "#9ca3af",
          textAlign: "center",
          lineHeight: 1.5,
        }}
      >
        {description}
      </p>
    </div>
  );
};

// Code snippet animation
const CodeSnippet: React.FC<{ startFrame: number }> = ({ startFrame }) => {
  const frame = useCurrentFrame();
  const localFrame = frame - startFrame;

  if (localFrame < 0) return null;

  const lines = [
    { text: "name: MyIndexer", color: colors.rust400 },
    { text: "project_type: no-code", color: colors.amber400 },
    { text: "", color: colors.white },
    { text: "networks:", color: colors.rust400 },
    { text: "  - name: ethereum", color: colors.white },
    { text: "    chain_id: 1", color: colors.amber400 },
    { text: "", color: colors.white },
    { text: "contracts:", color: colors.rust400 },
    { text: "  - name: USDC", color: colors.white },
  ];

  const opacity = interpolate(localFrame, [0, 15], [0, 1], {
    extrapolateRight: "clamp",
  });

  const scale = interpolate(localFrame, [0, 20], [0.9, 1], {
    extrapolateRight: "clamp",
    easing: Easing.out(Easing.cubic),
  });

  return (
    <div
      style={{
        opacity,
        transform: `scale(${scale})`,
        background: colors.midnight,
        borderRadius: 16,
        padding: 24,
        border: `2px solid ${colors.rust500}40`,
        boxShadow: `0 20px 60px -15px ${colors.rust500}30`,
        minWidth: 400,
      }}
    >
      <div style={{ display: "flex", gap: 8, marginBottom: 16 }}>
        <div style={{ width: 12, height: 12, borderRadius: "50%", background: "#ff5f56" }} />
        <div style={{ width: 12, height: 12, borderRadius: "50%", background: "#ffbd2e" }} />
        <div style={{ width: 12, height: 12, borderRadius: "50%", background: "#27c93f" }} />
      </div>
      <div style={{ fontFamily: "JetBrains Mono, monospace", fontSize: 14 }}>
        {lines.map((line, i) => {
          const lineProgress = interpolate(
            localFrame,
            [10 + i * 5, 15 + i * 5],
            [0, 1],
            { extrapolateLeft: "clamp", extrapolateRight: "clamp" }
          );
          return (
            <div
              key={i}
              style={{
                opacity: lineProgress,
                transform: `translateX(${(1 - lineProgress) * 20}px)`,
                color: line.color,
                height: 24,
              }}
            >
              {line.text}
            </div>
          );
        })}
      </div>
    </div>
  );
};

// Stats counter animation
const StatsCounter: React.FC<{
  label: string;
  value: string;
  startFrame: number;
  index: number;
}> = ({ label, value, startFrame, index }) => {
  const frame = useCurrentFrame();
  const { fps } = useVideoConfig();
  const localFrame = frame - startFrame;

  if (localFrame < 0) return null;

  const scale = spring({
    frame: localFrame - index * 5,
    fps,
    config: { damping: 10, stiffness: 100 },
  });

  const opacity = interpolate(localFrame, [index * 5, 10 + index * 5], [0, 1], {
    extrapolateRight: "clamp",
  });

  return (
    <div
      style={{
        display: "flex",
        flexDirection: "column",
        alignItems: "center",
        opacity,
        transform: `scale(${scale})`,
      }}
    >
      <span
        style={{
          fontFamily: "Sora, sans-serif",
          fontSize: 48,
          fontWeight: 800,
          background: `linear-gradient(135deg, ${colors.rust500}, ${colors.amber400})`,
          WebkitBackgroundClip: "text",
          WebkitTextFillColor: "transparent",
        }}
      >
        {value}
      </span>
      <span
        style={{
          fontFamily: "DM Sans, sans-serif",
          fontSize: 16,
          color: "#9ca3af",
          marginTop: 4,
        }}
      >
        {label}
      </span>
    </div>
  );
};

// Main title with gradient
const MainTitle: React.FC<{ startFrame: number }> = ({ startFrame }) => {
  const frame = useCurrentFrame();
  const { fps } = useVideoConfig();
  const localFrame = frame - startFrame;

  if (localFrame < 0) return null;

  const scale = spring({
    frame: localFrame,
    fps,
    config: { damping: 15, stiffness: 80 },
  });

  const opacity = interpolate(localFrame, [0, 15], [0, 1], {
    extrapolateRight: "clamp",
  });

  return (
    <div
      style={{
        textAlign: "center",
        opacity,
        transform: `scale(${scale})`,
      }}
    >
      <h1
        style={{
          fontFamily: "Sora, sans-serif",
          fontSize: 64,
          fontWeight: 800,
          color: colors.white,
          lineHeight: 1.1,
          margin: 0,
        }}
      >
        Blazing Fast
      </h1>
      <h1
        style={{
          fontFamily: "Sora, sans-serif",
          fontSize: 64,
          fontWeight: 800,
          background: `linear-gradient(135deg, ${colors.rust500}, ${colors.amber400})`,
          WebkitBackgroundClip: "text",
          WebkitTextFillColor: "transparent",
          lineHeight: 1.1,
          margin: 0,
        }}
      >
        EVM Indexing
      </h1>
    </div>
  );
};

// CTA Button
const CTAButton: React.FC<{ startFrame: number }> = ({ startFrame }) => {
  const frame = useCurrentFrame();
  const { fps } = useVideoConfig();
  const localFrame = frame - startFrame;

  if (localFrame < 0) return null;

  const scale = spring({
    frame: localFrame,
    fps,
    config: { damping: 10, stiffness: 100 },
  });

  const pulse = interpolate(
    Math.sin((frame - startFrame) / 10),
    [-1, 1],
    [1, 1.05]
  );

  return (
    <div
      style={{
        display: "flex",
        gap: 20,
        justifyContent: "center",
        transform: `scale(${scale})`,
      }}
    >
      <div
        style={{
          padding: "16px 40px",
          background: `linear-gradient(135deg, ${colors.rust500}, ${colors.rust600})`,
          borderRadius: 12,
          transform: `scale(${pulse})`,
          boxShadow: `0 10px 30px -10px ${colors.rust500}80`,
        }}
      >
        <span
          style={{
            fontFamily: "DM Sans, sans-serif",
            fontSize: 20,
            fontWeight: 600,
            color: colors.white,
          }}
        >
          Get Started Free
        </span>
      </div>
    </div>
  );
};

// Scene components
const IntroScene: React.FC = () => {
  return (
    <AbsoluteFill
      style={{
        display: "flex",
        flexDirection: "column",
        alignItems: "center",
        justifyContent: "center",
        gap: 20,
      }}
    >
      <AnimatedBackground />
      <Logo />
      <Tagline text="Built in Rust for Maximum Performance" startFrame={20} />
    </AbsoluteFill>
  );
};

const TitleScene: React.FC = () => {
  return (
    <AbsoluteFill
      style={{
        display: "flex",
        flexDirection: "column",
        alignItems: "center",
        justifyContent: "center",
        gap: 30,
      }}
    >
      <AnimatedBackground />
      <MainTitle startFrame={0} />
      <Tagline text="Index any EVM chain with simple YAML configuration" startFrame={25} />
    </AbsoluteFill>
  );
};

const FeaturesScene: React.FC = () => {
  const features = [
    {
      icon: "📄",
      title: "No-Code Config",
      description: "Define indexing logic with simple YAML files",
    },
    {
      icon: "⚡",
      title: "Built for Speed",
      description: "Written in Rust for maximum performance",
    },
    {
      icon: "🔗",
      title: "Any EVM Chain",
      description: "Ethereum, Polygon, Arbitrum & more",
    },
  ];

  return (
    <AbsoluteFill
      style={{
        display: "flex",
        flexDirection: "column",
        alignItems: "center",
        justifyContent: "center",
      }}
    >
      <AnimatedBackground />
      <div style={{ display: "flex", gap: 24 }}>
        {features.map((feature, i) => (
          <FeatureCard
            key={feature.title}
            {...feature}
            index={i}
            startFrame={i * 10}
          />
        ))}
      </div>
    </AbsoluteFill>
  );
};

const Features2Scene: React.FC = () => {
  const features = [
    {
      icon: "📊",
      title: "GraphQL API",
      description: "Auto-generated APIs for your indexed data",
    },
    {
      icon: "📡",
      title: "Real-time Streams",
      description: "Stream to Kafka, Redis, webhooks & more",
    },
    {
      icon: "🛠️",
      title: "Extensible",
      description: "Full Rust framework when you need control",
    },
  ];

  return (
    <AbsoluteFill
      style={{
        display: "flex",
        flexDirection: "column",
        alignItems: "center",
        justifyContent: "center",
      }}
    >
      <AnimatedBackground />
      <div style={{ display: "flex", gap: 24 }}>
        {features.map((feature, i) => (
          <FeatureCard
            key={feature.title}
            {...feature}
            index={i}
            startFrame={i * 10}
          />
        ))}
      </div>
    </AbsoluteFill>
  );
};

const CodeScene: React.FC = () => {
  return (
    <AbsoluteFill
      style={{
        display: "flex",
        flexDirection: "column",
        alignItems: "center",
        justifyContent: "center",
        gap: 30,
      }}
    >
      <AnimatedBackground />
      <div
        style={{
          fontFamily: "Sora, sans-serif",
          fontSize: 36,
          fontWeight: 700,
          color: colors.white,
          textAlign: "center",
        }}
      >
        Configure in seconds
      </div>
      <CodeSnippet startFrame={15} />
    </AbsoluteFill>
  );
};

const StatsScene: React.FC = () => {
  const stats = [
    { label: "GitHub Stars", value: "1.2K+" },
    { label: "Latest Version", value: "v0.33" },
    { label: "MIT License", value: "Free" },
  ];

  return (
    <AbsoluteFill
      style={{
        display: "flex",
        flexDirection: "column",
        alignItems: "center",
        justifyContent: "center",
        gap: 40,
      }}
    >
      <AnimatedBackground />
      <div
        style={{
          fontFamily: "Sora, sans-serif",
          fontSize: 36,
          fontWeight: 700,
          color: colors.white,
        }}
      >
        Trusted by developers worldwide
      </div>
      <div style={{ display: "flex", gap: 80 }}>
        {stats.map((stat, i) => (
          <StatsCounter
            key={stat.label}
            {...stat}
            startFrame={15}
            index={i}
          />
        ))}
      </div>
    </AbsoluteFill>
  );
};

const OutroScene: React.FC = () => {
  return (
    <AbsoluteFill
      style={{
        display: "flex",
        flexDirection: "column",
        alignItems: "center",
        justifyContent: "center",
        gap: 30,
      }}
    >
      <AnimatedBackground />
      <Logo />
      <div
        style={{
          fontFamily: "Sora, sans-serif",
          fontSize: 42,
          fontWeight: 700,
          color: colors.white,
          textAlign: "center",
        }}
      >
        Start indexing today
      </div>
      <CTAButton startFrame={20} />
      <div
        style={{
          fontFamily: "DM Sans, sans-serif",
          fontSize: 18,
          color: "#9ca3af",
          marginTop: 20,
        }}
      >
        rindexer.xyz
      </div>
    </AbsoluteFill>
  );
};

// Main composition
export const RindexerPromo: React.FC = () => {
  return (
    <AbsoluteFill style={{ backgroundColor: colors.midnight }}>
      <Sequence from={0} durationInFrames={60}>
        <IntroScene />
      </Sequence>

      <Sequence from={60} durationInFrames={75}>
        <TitleScene />
      </Sequence>

      <Sequence from={135} durationInFrames={70}>
        <FeaturesScene />
      </Sequence>

      <Sequence from={205} durationInFrames={70}>
        <Features2Scene />
      </Sequence>

      <Sequence from={275} durationInFrames={70}>
        <CodeScene />
      </Sequence>

      <Sequence from={345} durationInFrames={50}>
        <StatsScene />
      </Sequence>

      <Sequence from={395} durationInFrames={55}>
        <OutroScene />
      </Sequence>
    </AbsoluteFill>
  );
};
