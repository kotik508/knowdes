# Working together

## Branches and pull requests

- Nobody pushes to `main` directly. Work on a branch named after the issue, for example `12-raft-election`.
- Open a pull request early and link the issue (`Closes #12`).
- Someone else reviews and merges it. Then the history also shows who did what, which we need for part 8 of the report.
- CI runs ruff and pytest on every pull request. Merge only when it is green.

## Issues

Every piece of work is an issue with one person assigned and a milestone (22.11., 29.11., 6.12.). Labels name the area: `raft`, `store`, `router`, `2pc`, `placement`, `clients`, `console`, `deploy`, `measurements`, `report`.

## Code

- Clear names over comments, comments where the reason is not obvious.
- Every module starts with a docstring that says which part of the design plan it implements.
- Format and lint with `uv run ruff format` and `uv run ruff check` before you push.

## AI usage

The final report needs a description of how we used AI tools, with prompts where AI produced code or text. If you use AI for something in this repository, add a short entry to `docs/ai-usage.md` in the same pull request: what, which tool and the prompt.
