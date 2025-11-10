import { defineConfig } from "astro/config";
import tailwindcss from "@tailwindcss/vite";
import starlight from "@astrojs/starlight";

// https://astro.build/config
export default defineConfig({
  integrations: [
    starlight({
      title: "Amp",
      customCss: ["./src/styles/global.css"],
      expressiveCode: {
        themes: ["material-theme-ocean", "material-theme-lighter"],
      },
      logo: {
        src: "./public/logo.svg",
      }
    }),
  ],
  vite: {
    plugins: [tailwindcss()],
  },
});
