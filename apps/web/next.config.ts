import type { NextConfig } from "next";
import { resolveApiBase } from "./src/lib/deployment";

resolveApiBase({ environment: process.env.NEXT_PUBLIC_APP_ENV, apiUrl: process.env.NEXT_PUBLIC_API_BASE_URL,
  onPages: process.env.CF_PAGES, pagesBranch: process.env.CF_PAGES_BRANCH, productionBranch: process.env.BEEDLE_PRODUCTION_BRANCH });

const nextConfig: NextConfig = {
  reactStrictMode: true
};

export default nextConfig;
