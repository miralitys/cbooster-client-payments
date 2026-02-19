import path from "node:path";
import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

const backendTarget = process.env.VITE_BACKEND_URL || "http://localhost:10000";

// https://vite.dev/config/
export default defineConfig({
  base: "/app/",
  plugins: [react()],
  resolve: {
    alias: {
      "@": path.resolve(__dirname, "src"),
    },
  },
  server: {
    proxy: {
      "/api": {
        target: backendTarget,
        changeOrigin: true,
      },
      "/login": {
        target: backendTarget,
        changeOrigin: true,
      },
      "/logout": {
        target: backendTarget,
        changeOrigin: true,
      },
    },
  },
});
