import { defineConfig } from "vite";

export default defineConfig({
  base: "/app/",
  server: {
    host: "0.0.0.0",
    port: 5173
  }
});
