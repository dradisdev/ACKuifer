# ACKuifer — Claude Code Instructions

## Git Workflow
Commit directly to the current branch. Do not create feature branches unless explicitly asked. When on main, commit directly to main.

## Commit Messages
Use conventional commits: feat:, fix:, chore:, docs:

## Stack
Python 3.11+, FastAPI, PostgreSQL, Playwright, Mapbox GL JS, Railway deployment.

## Key Rules
- Never commit .env or any file containing secrets
- Never hardcode constants — use named constants from app/config.py
- House numbers are never displayed publicly — street name only
- Run existing tests before committing if tests exist
