"""Get the current git version to append to the csv file so
we know what code was used to make that csv file.
"""

import subprocess
import os


def strip_package_version_number():
    """Generate the version info from git tags and commits."""
    try:
        repo_dir = os.path.abspath(os.path.join(os.path.dirname(__file__)))
        current_branch = subprocess.check_output(
            ["git", "rev-parse", "--abbrev-ref", "HEAD"],
            cwd=repo_dir, stderr=subprocess.PIPE)
        current_branch = current_branch.strip().decode("utf-8")
        git_describe = subprocess.check_output(
            ["git", "describe", "--always", "--dirty"],
            cwd=repo_dir, stderr=subprocess.PIPE)
        git_describe = git_describe.strip().decode("utf-8").lstrip("v")

        if current_branch == "master":
            # We're on master or on a stable/release branch, so the
            # version number is just the 'git describe' output.
            version = git_describe
        else:
            # We're on a temporary branch, so make a version number that
            # sorts as includes the branch we're on with the release.
            parts = git_describe.split("-")
            parts[0] = "{}~BRANCH:{}".format(parts[0], current_branch)
            version = "-".join(parts)

    except (subprocess.CalledProcessError, OSError):
        # git failed to give us a version number, give up.
        version = "unknown"
    return version

if __name__ == "__main__":
    print (strip_package_version_number())