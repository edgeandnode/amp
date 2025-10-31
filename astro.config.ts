import { defineConfig } from "astro/config";
import tailwindcss from "@tailwindcss/vite";
import starlight from "@astrojs/starlight";

// https://astro.build/config
export default defineConfig({
  integrations: [
    starlight({
      title: "Amp",
    }),
  ],
  vite: {
    plugins: [tailwindcss()],
  },
});
