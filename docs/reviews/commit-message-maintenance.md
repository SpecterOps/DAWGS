# Commit message maintenance

Reference for the reviewed historical message map and its maintenance script.

[Documentation index](../README.md) · [Project overview](../../README.md)

The reviewed [message map](../../scripts/commit_messages.json) covers all **83 commits**
from `21c7bafc1e1d91c3507dff86da9fe3a81c96a76d` through
`cdfadee7c510bbedcb877707ae209b802aa6e77e`, inclusive. Messages follow
Conventional Commits and describe the actual diffs, checked against
[the priority plan](variance_findings_priority.md) and
[the completed work ledger](variance_findings_priority_complete.md). Review notes
identify mislabeled findings, combined repairs and reproduction-only changes.

Preview the messages, then apply them with an interactive rebase:

```bash
python3 scripts/rewrite_commit_messages.py
python3 scripts/rewrite_commit_messages.py --apply
```

Run from the branch at the reviewed tip above. The script marks each commit as
`reword` and supplies its reviewed message to Git's editor. Git uses your ambient
signing configuration. Local tracked edits are handled by rebase's `--autostash`.
If the rebase stops, follow Git's instructions and resume with the same message
editor:

```bash
python3 scripts/rewrite_commit_messages.py --continue
```

The maintenance tests use disposable repositories and require Python 3 and Git:

```bash
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s scripts -p 'test_*.py' -v
```
