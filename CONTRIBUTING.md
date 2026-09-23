# Contributing

## Branch workflow

- `main` — the default integration branch.
- `feat/<short-name>` — a specific new feature.
- `fix/<short-name>` — a bug fix.
- `docs/<short-name>` — documentation updates.
- `chore/<short-name>` — dependencies and maintenance.

Start each task from the latest `main`:

```bash
git switch main
git pull --ff-only
git switch -c feat/short-name
```

Open a pull request targeting `main`. Explain the resulting behavior, validation and configuration changes. Delete completed branches after merging. Preserve existing work branches until their changes have been reviewed and integrated.

## Before submitting

- Review the diff and check the affected component.
- Keep tokens, `.env` files, user databases and logs out of commits.
- Document new configuration variables with placeholder values.
- Update the README when setup or features change.

Suggested commit prefixes: `feat:`, `fix:`, `docs:` and `chore:`.
