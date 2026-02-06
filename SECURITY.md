# Security Policy

## Supported versions

Security updates are provided for the latest released version.

## Reporting a vulnerability

If you believe you’ve found a security issue, please avoid opening a public GitHub issue.

Instead, contact the maintainers (for example, via a private message) with:

- a description of the issue
- steps to reproduce
- impact assessment (what data could be exposed or modified)
- suggested fix (if you have one)

We will acknowledge receipt and aim to provide an initial response within a few business days.

## Data handling notes

`prompt-search` reads Codex session history stored locally under `~/.codex/sessions/**/*` and stores an index database locally under `~/.prompt-search/` by default.

