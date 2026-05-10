<!-- BEGIN:nextjs-agent-rules -->
# This is NOT the Next.js you know

This version has breaking changes — APIs, conventions, and file structure may all differ from your training data. Read the relevant guide in `node_modules/next/dist/docs/` before writing any code. Heed deprecation notices.
<!-- END:nextjs-agent-rules -->

<!-- BEGIN:design-context -->
# Design Context

This project has strategic and visual design context files at the repository root:

- **PRODUCT.md** — Register (product), target users (billing analysts/supervisors), brand personality (Precise / Clinical / Trusted), design principles, anti-references.
- **DESIGN.md** — Creative North Star ("The Diagnostic Console"), color strategy (full palette, cool slate-blue anchor), typography (grotesk sans), elevation, Do's and Don'ts.

Read these files before generating new screens or components. They are the source of truth for visual decisions.

**Available tools for frontend work:**
- **impeccable skill** — always load when designing or iterating on the UI ($impeccable craft, shape, critique, polish, etc.)
- **shadcn skill + MCP** — primary UI component system. Use shadcn MCP tools (`shadcn_search_items_in_registries`, `shadcn_view_items_in_registries`, `shadcn_get_add_command_for_items`) to find, inspect, and add components. Do not hand-roll components that shadcn provides.
- **Better Auth skill + MCP** — use for all auth-related work. The `better-auth_search_docs`, `better-auth_get_doc` tools provide authoritative documentation for server config, client setup, Google OAuth, session management, database adapters, and plugins.
<!-- END:design-context -->
