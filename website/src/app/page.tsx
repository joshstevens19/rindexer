import { Hero } from "@/components/Hero";
import { Features } from "@/components/Features";
import { VideoSection } from "@/components/VideoSection";
import { CodeDemo } from "@/components/CodeDemo";
import { Stats } from "@/components/Stats";
import { UseCases } from "@/components/UseCases";
import { Integrations } from "@/components/Integrations";
import { QuickStart } from "@/components/QuickStart";
import { CTA } from "@/components/CTA";

export default function Home() {
  return (
    <>
      <Hero />
      <Features />
      <VideoSection />
      <CodeDemo />
      <Stats />
      <UseCases />
      <Integrations />
      <QuickStart />
      <CTA />
    </>
  );
}
