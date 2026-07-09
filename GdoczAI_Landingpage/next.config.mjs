/** @type {import('next').NextConfig} */
const nextConfig = {
  reactStrictMode: true,
  transpilePackages: ["antd", "@ant-design/icons", "@ant-design/nextjs-registry"],
  // Every page in this project is statically generated (SSG) at build time
  // because none of them read request-time data (cookies/headers/searchParams).
  // If you later add a page that needs per-request rendering (SSR), Next.js
  // will switch that route to the Node.js runtime automatically — you don't
  // need to change anything here for that.
  images: {
    remotePatterns: [{ protocol: "https", hostname: "gramosoft.tech" }],
  },
};

export default nextConfig;
