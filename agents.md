## Purpose

- Contribution guidelines for AI/droid agents interacting with this repo.
- Keep changes minimal, safe, and consistent with repo coding standards.

## Setup / Requirements

- This repo is a part of a larger monorepo. The root of monorepo is to be set as `export WORKSPACE=../../../../../`
- To Build the code changes, do `cd $WORKSPACE && make`
- Use project Go toolchain and existing dependencies; avoid adding new ones unless required.
- No credentials or secrets in code, logs, or configs.
- gometa repository is a sub-tool for `indexing` repository. indexing should be a sibling project
  in parent directory. Follow scripts and tooling from `indexing/agents.MD`

## Conventions / Policies

- Respect existing patterns and naming; follow surrounding style.
- Keep comments minimal and functional; no README/docs edits unless requested.
- Ensure safety: no unvetted downloads, no external code execution.
- Keep logging changes to a minimum to avoid log flooding in production. Use Debug and Trace if
  required for extra information
