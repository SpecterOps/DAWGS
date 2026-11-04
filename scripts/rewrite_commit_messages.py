#!/usr/bin/env python3
"""Preview reviewed commit messages, or apply them with an interactive rebase."""

import argparse
import json
import os
import shlex
import subprocess
import sys
from pathlib import Path


def git(*args):
    return subprocess.check_output(["git", *args], text=True).strip()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--manifest",
        type=Path,
        default=Path(__file__).with_name("commit_messages.json"),
        help="reviewed message map",
    )
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument(
        "--apply", action="store_true", help="rebase and reword the reviewed commits"
    )
    mode.add_argument(
        "--continue",
        dest="resume",
        action="store_true",
        help="continue an interrupted rebase with the message editor",
    )
    mode.add_argument("--sequence-editor", type=Path, help=argparse.SUPPRESS)
    mode.add_argument("--message-editor", type=Path, help=argparse.SUPPRESS)
    args = parser.parse_args()
    plan = json.loads(args.manifest.read_text(encoding="utf-8"))

    if args.sequence_editor:
        args.sequence_editor.write_text(
            "".join(
                f"reword {entry['commit']} {entry['original_subject']}\n"
                for entry in plan["commits"]
            ),
            encoding="utf-8",
        )
        return 0
    if args.message_editor:
        # Git records the current reword command before invoking its editor.
        done = Path(git("rev-parse", "--git-path", "rebase-merge/done"))
        original = done.read_text(encoding="utf-8").splitlines()[-1].split()[1]
        entry = next(row for row in plan["commits"] if row["commit"] == original)
        args.message_editor.write_text(entry["message"], encoding="utf-8")
        return 0

    if not args.apply and not args.resume:
        for entry in plan["commits"]:
            print(f"{entry['commit']}\n{entry['message']}")
        print("Preview only. Run with --apply to rebase and reword these commits.")
        return 0

    if args.apply and git("rev-parse", "HEAD") != plan["tip"]:
        parser.error(f"check out the reviewed tip {plan['tip']} before applying")
    editor = [
        sys.executable,
        str(Path(__file__).resolve()),
        "--manifest",
        str(args.manifest.resolve()),
    ]
    env = os.environ.copy()
    env["GIT_SEQUENCE_EDITOR"] = shlex.join([*editor, "--sequence-editor"])
    env["GIT_EDITOR"] = shlex.join([*editor, "--message-editor"])
    command = (
        ["--continue"]
        if args.resume
        else ["--interactive", "--autostash", "--keep-empty", plan["base"]]
    )
    return subprocess.call(["git", "rebase", *command], env=env)


if __name__ == "__main__":
    sys.exit(main())
