# Databricks MCP setup

This repo wires up Databricks' managed MCP server via [.mcp.json](../.mcp.json), so
Claude Code can query the workspace directly. Claude Code resolves `${VAR}`
placeholders in `.mcp.json` from the process environment **at startup**, so the
environment has to be in place *before* Claude Code (or the editor it's running
inside) launches.

## How it's wired

- [.envrc](../.envrc) (gitignored) holds `DATABRICKS_HOST` and `DATABRICKS_TOKEN`
  for the target workspace.
- [direnv](https://direnv.net/) loads `.envrc` automatically whenever your shell
  `cd`s into the project directory.
- [.mcp.json](../.mcp.json) expands `${DATABRICKS_HOST}` / `${DATABRICKS_TOKEN}`
  to configure the `dbsql` MCP server.

If the editor is launched some other way (Dock icon, Spotlight, "Open Recent"),
it won't inherit a shell environment at all, and the MCP server will fail to
resolve its variables.

## One-time setup

1. Install direnv and hook it into your shell (e.g. for zsh, add
   `eval "$(direnv hook zsh)"` to `~/.zshrc`, then restart the shell).
2. Make sure `.envrc` is listed in `.gitignore`
3. Fill in your own values in `.envrc`.
4. From a terminal, `cd` into the project directory, then run `direnv allow`
   to authorize the file. direnv re-prompts for approval any time `.envrc`
   changes, as a safeguard against silently executing edited shell code.

## Every time you work on this project

1. Open a terminal and `cd` into the project directory — direnv auto-exports
   the Databricks env vars into that shell.
2. Launch your editor **from that same shell** (e.g. `code .` for VS Code) so
   it inherits the environment.

Once running, `/mcp` (Claude Code) should show the `dbsql` server as connected
rather than failing to resolve.
