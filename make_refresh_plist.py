"""Generate the launchd plist for the daily refresh job.

Uses sys.executable (the framework Python that has duckdb/requests installed) rather than
/usr/bin/python3 — the sibling CIMT app's job silently failed on the system python for
exactly this reason. The repo already lives directly under $HOME (not Desktop/iCloud), so
launchd can read it without Full Disk Access grants.

Usage:
    python3 make_refresh_plist.py            # writes com.jasonkirby.us-lobbying-refresh.plist
    # then install:
    #   cp com.jasonkirby.us-lobbying-refresh.plist ~/Library/LaunchAgents/
    #   launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.jasonkirby.us-lobbying-refresh.plist
    #   launchctl enable gui/$(id -u)/com.jasonkirby.us-lobbying-refresh
"""

import os
import plistlib
import sys

LABEL = "com.jasonkirby.us-lobbying-refresh"
REPO = os.path.dirname(os.path.abspath(__file__))
HOUR, MINUTE = 7, 45  # daily, before the workday; LDA posts filings continuously


def main() -> None:
    if "/Desktop/" in REPO or "/Mobile Documents/" in REPO:
        sys.exit(f"refusing: {REPO} is under Desktop/iCloud — launchd will hit TCC problems")

    plist = {
        "Label": LABEL,
        "ProgramArguments": [sys.executable, os.path.join(REPO, "refresh.py")],
        "WorkingDirectory": REPO,
        "StartCalendarInterval": {"Hour": HOUR, "Minute": MINUTE},
        "StandardOutPath": os.path.join(REPO, "refresh.log"),
        "StandardErrorPath": os.path.join(REPO, "refresh.log"),
        "EnvironmentVariables": {"PATH": "/usr/local/bin:/usr/bin:/bin"},
    }
    out = os.path.join(REPO, f"{LABEL}.plist")
    with open(out, "wb") as f:
        plistlib.dump(plist, f)
    print(f"wrote {out} (runs daily {HOUR:02d}:{MINUTE:02d} via {sys.executable})")
    print("install:")
    print(f"  cp {out} ~/Library/LaunchAgents/")
    print(f"  launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/{LABEL}.plist")
    print(f"  launchctl enable gui/$(id -u)/{LABEL}")


if __name__ == "__main__":
    main()
