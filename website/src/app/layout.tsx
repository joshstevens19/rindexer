import type { Metadata } from "next";
import "./globals.css";
import { Header } from "@/components/Header";
import { Footer } from "@/components/Footer";

export const metadata: Metadata = {
  title: "rindexer - High-Performance EVM Blockchain Indexing",
  description:
    "A no-code or framework to build blazing fast EVM indexers. Built in Rust for maximum performance. Index any EVM chain with YAML configuration and get instant GraphQL APIs.",
  keywords: [
    "blockchain indexer",
    "EVM",
    "Rust",
    "GraphQL",
    "ethereum",
    "no-code",
    "blockchain",
    "web3",
    "indexing",
    "smart contracts",
    "crypto",
    "dApp",
  ],
  authors: [{ name: "Josh Stevens", url: "https://github.com/joshstevens19" }],
  creator: "Josh Stevens",
  publisher: "rindexer",
  metadataBase: new URL("https://rindexer.xyz"),
  openGraph: {
    type: "website",
    title: "rindexer - High-Performance EVM Blockchain Indexing",
    description:
      "A no-code or framework to build blazing fast EVM indexers. Built in Rust for maximum performance.",
    url: "https://rindexer.xyz",
    siteName: "rindexer",
    images: [
      {
        url: "/og-image.png",
        width: 1200,
        height: 630,
        alt: "rindexer - High-Performance EVM Blockchain Indexing",
      },
    ],
    locale: "en_US",
  },
  twitter: {
    card: "summary_large_image",
    title: "rindexer - High-Performance EVM Blockchain Indexing",
    description:
      "A no-code or framework to build blazing fast EVM indexers. Built in Rust for maximum performance.",
    images: ["/og-image.png"],
  },
  robots: {
    index: true,
    follow: true,
    googleBot: {
      index: true,
      follow: true,
      "max-video-preview": -1,
      "max-image-preview": "large",
      "max-snippet": -1,
    },
  },
  alternates: {
    canonical: "https://rindexer.xyz",
  },
  category: "technology",
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="en">
      <head>
        <link rel="icon" href="/favicon.png" type="image/png" />
        <script
          type="application/ld+json"
          dangerouslySetInnerHTML={{
            __html: JSON.stringify({
              "@context": "https://schema.org",
              "@type": "SoftwareApplication",
              name: "rindexer",
              description:
                "A no-code or framework to build blazing fast EVM indexers. Built in Rust for maximum performance.",
              applicationCategory: "DeveloperApplication",
              operatingSystem: "Linux, macOS, Windows",
              offers: {
                "@type": "Offer",
                price: "0",
                priceCurrency: "USD",
              },
              author: {
                "@type": "Person",
                name: "Josh Stevens",
                url: "https://github.com/joshstevens19",
              },
              codeRepository: "https://github.com/joshstevens19/rindexer",
              license: "https://opensource.org/licenses/MIT",
              softwareVersion: "0.33.0",
              programmingLanguage: "Rust",
              keywords:
                "blockchain, indexer, EVM, Ethereum, Rust, GraphQL, no-code",
            }),
          }}
        />
      </head>
      <body className="bg-cream font-sans text-gray-900 antialiased">
        <Header />
        <main>{children}</main>
        <Footer />
      </body>
    </html>
  );
}
