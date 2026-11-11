"""Exercise automated rewording in disposable repositories."""

import json
import os
import shlex
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

SCRIPT = Path(__file__).with_name("rewrite_commit_messages.py").resolve()


class RewordTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory(prefix="dawgs reword test ")
        self.addCleanup(self.directory.cleanup)
        self.repo = Path(self.directory.name)
        self.env = {k: v for k, v in os.environ.items() if not k.startswith("GIT_")}
        self.env.update(GIT_CONFIG_NOSYSTEM="1", GIT_CONFIG_GLOBAL=os.devnull)
        self.git("init", "-q", "--initial-branch=topic")
        self.git("config", "user.name", "Rebase Test")
        self.git("config", "user.email", "rebase@example.test")
        self.git("config", "commit.gpgsign", "false")
        self.base = self.commit("base", "base\n")
        self.originals = [
            self.commit("wip", "first\n"),
            self.commit("wip", "second\n"),
            self.commit("wip", "second\n"),
        ]
        self.plan = {
            "base": self.base,
            "tip": self.originals[-1],
            "commits": [
                {
                    "commit": oid,
                    "original_subject": "wip",
                    "message": f"fix(test): message {index}\n\nRetain café.\n",
                }
                for index, oid in enumerate(self.originals)
            ],
        }
        self.manifest = self.repo / "reviewed messages.json"
        self.manifest.write_text(json.dumps(self.plan), encoding="utf-8")

    def git(self, *args):
        return subprocess.check_output(
            ["git", *args],
            cwd=self.repo,
            env=self.env,
            text=True,
            stderr=subprocess.PIPE,
        ).strip()

    def commit(self, subject, content):
        (self.repo / "tracked.txt").write_text(content)
        self.git("add", "tracked.txt")
        self.git("commit", "--allow-empty", "-qm", subject)
        return self.git("rev-parse", "HEAD")

    def invoke(self, *args):
        return subprocess.run(
            [sys.executable, str(SCRIPT), "--manifest", str(self.manifest), *args],
            cwd=self.repo,
            env=self.env,
            text=True,
            capture_output=True,
        )

    def test_preview_leaves_history_unchanged(self):
        result = self.invoke()
        self.assertEqual(0, result.returncode, result.stderr)
        self.assertIn("Preview only", result.stdout)
        self.assertIn("Retain café", result.stdout)
        self.assertEqual(self.plan["tip"], self.git("rev-parse", "HEAD"))

    def test_reword_preserves_changes_authors_and_empty_commits(self):
        result = self.invoke("--apply")
        self.assertEqual(0, result.returncode, result.stdout + result.stderr)
        rewritten = self.git("rev-list", "--reverse", f"{self.base}..HEAD").splitlines()
        self.assertEqual(3, len(rewritten))
        for entry, new in zip(self.plan["commits"], rewritten):
            self.assertEqual(
                entry["message"].strip(), self.git("show", "-s", "--format=%B", new)
            )
            for spec in ("%T", "%an%n%ae%n%aI"):
                self.assertEqual(
                    self.git("show", "-s", f"--format={spec}", entry["commit"]),
                    self.git("show", "-s", f"--format={spec}", new),
                )
        self.assertEqual("", self.git("status", "--short", "--untracked-files=no"))

    def test_rebase_autostashes_local_edits(self):
        (self.repo / "tracked.txt").write_text("local edits\n")
        result = self.invoke("--apply")
        self.assertEqual(0, result.returncode, result.stdout + result.stderr)
        self.assertEqual("local edits\n", (self.repo / "tracked.txt").read_text())

    def test_unreviewed_tip_is_rejected(self):
        newer = self.commit("new work", "new work\n")
        result = self.invoke("--apply")
        self.assertNotEqual(0, result.returncode)
        self.assertIn("reviewed tip", result.stderr)
        self.assertEqual(newer, self.git("rev-parse", "HEAD"))

    def test_continue_retains_the_message_editor(self):
        # Stop before the first commit so --continue exercises the same reword
        # sequence that an interrupted run would have left in Git's todo file.
        todo = self.repo / "todo"
        todo.write_text(
            "break\n" + "".join(f"reword {oid} wip\n" for oid in self.originals)
        )
        env = self.env.copy()
        env["GIT_SEQUENCE_EDITOR"] = shlex.join(["cp", str(todo)])
        subprocess.run(
            ["git", "rebase", "--interactive", self.base],
            cwd=self.repo,
            env=env,
            check=True,
            capture_output=True,
        )
        result = self.invoke("--continue")
        self.assertEqual(0, result.returncode, result.stdout + result.stderr)
        self.assertEqual("fix(test): message 2", self.git("log", "-1", "--format=%s"))


class ReviewedMessagesTests(unittest.TestCase):
    def test_reviewed_range_and_corrected_findings(self):
        plan = json.loads(SCRIPT.with_name("commit_messages.json").read_text())
        self.assertEqual(83, len(plan["commits"]))
        self.assertEqual("21c7bafc1e1d91c3507dff86da9fe3a81c96a76d", plan["start"])
        self.assertEqual("cdfadee7c510bbedcb877707ae209b802aa6e77e", plan["tip"])
        by_prefix = {row["commit"][:8]: row for row in plan["commits"]}
        for prefix, finding in {
            "1703d69f": "DAWGS-084",
            "5ffb52a2": "DAWGS-026",
            "dbedfe58": "DAWGS-076",
            "be062e7e": "DAWGS-103",
            "68ac7243": "DAWGS-059",
        }.items():
            self.assertIn(finding, by_prefix[prefix]["findings"])


if __name__ == "__main__":
    unittest.main()
