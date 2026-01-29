/** @type {import('next').NextConfig} */
const nextConfig = {
  webpack: (config) => {
    // Enable WebAssembly support (required for sql.js)
    config.experiments = {
      ...(config.experiments || {}),
      asyncWebAssembly: true,
    };

    // Tell webpack how to handle sql.js wasm + zip artifacts
    config.module.rules.push(
      {
        test: /\.wasm$/,
        type: "webassembly/async",
      },
      {
        test: /\.zip$/,
        type: "asset/resource",
      }
    );

    return config;
  },
};

module.exports = nextConfig;
