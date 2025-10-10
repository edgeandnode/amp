# AGENTS.md

This file provides guidance to coding agents when working with code in this repository.

## Project Overview

This is the landing page and installer distribution site for Amp, a tool for building and managing blockchain datasets. The site is built with Astro and serves the `ampup` installer script.

## Package Manager

Use `pnpm` for all package management operations (specified in package.json packageManager field).

## Common Commands

```sh
# Development server
pnpm dev

# Production build
pnpm build

# Preview production build
pnpm preview
```

## Architecture

- **Framework**: Astro static site with Starlight documentation
- **Landing Page**: `src/pages/index.astro` - main landing page at `/`
- **Documentation**: Powered by Astro Starlight at `/docs`
  - Content in `src/content/docs/`
  - Configured in `astro.config.ts` with `@astrojs/starlight` integration
  - Content collections configured in `src/content.config.ts`
- **Installer**: `public/install` - shell script that downloads and installs ampup binary
  - Detects platform (Linux/Darwin) and architecture (x86_64/aarch64)
  - Downloads from GitHub releases: `github.com/edgeandnode/amp/releases/latest`
  - Served at `https://ampup.sh/install`

## Key Components

- `src/pages/index.astro` - Landing page with installation command and link to docs
- `src/content/docs/docs.md` - Documentation homepage accessible at `/docs`
- `src/content.config.ts` - Starlight content collections configuration
- `public/install` - Platform-agnostic installer script that:
  - Performs platform/architecture detection
  - Downloads appropriate ampup binary from GitHub releases
  - Executes ampup initialization
  - Handles cleanup

## Documentation

Documentation is built with Astro Starlight and served at `/docs`. To add new documentation pages:
- Add `.md` files to `src/content/docs/`
- File paths map to URLs: `src/content/docs/foo.md` → `/docs/foo`
- Use frontmatter for page metadata (title, description)

The installer is the primary distribution mechanism referenced in README and served at the ampup.sh domain.
