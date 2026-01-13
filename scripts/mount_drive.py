#!/usr/bin/env python3
"""
Mount Google Drive for use in Colab terminal.

When run from terminal, this script handles the OAuth flow
by printing the auth URL and waiting for the user to complete it.

Usage:
    python scripts/mount_drive.py [--mount-point /content/drive]
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path


def is_drive_mounted(mount_point: str = "/content/drive") -> bool:
    """
    Check if Google Drive is already mounted.

    Args:
        mount_point: The mount point to check.

    Returns:
        True if Drive is mounted and accessible, False otherwise.
    """
    mydrive_path = Path(mount_point) / "MyDrive"
    return mydrive_path.exists() and mydrive_path.is_dir()


def mount_google_drive(mount_point: str = "/content/drive", force_remount: bool = False) -> bool:
    """
    Mount Google Drive using the Colab API.

    Args:
        mount_point: Where to mount Google Drive.
        force_remount: If True, remount even if already mounted.

    Returns:
        True if mount was successful, False otherwise.
    """
    # Check if already mounted
    if is_drive_mounted(mount_point) and not force_remount:
        print(f"✓ Google Drive already mounted at {mount_point}")
        return True

    try:
        # Import Colab drive module
        from google.colab import drive

        print("Mounting Google Drive...")
        print("If running from terminal, an authorization URL will be displayed.")
        print("Please open the URL in a browser and complete the OAuth flow.")
        print()

        # Mount the drive
        # When run from terminal, this will print the auth URL
        drive.mount(mount_point, force_remount=force_remount)

        # Verify mount was successful
        if is_drive_mounted(mount_point):
            print()
            print(f"✓ Google Drive successfully mounted at {mount_point}")
            return True
        else:
            print()
            print(f"✗ Mount command completed but {mount_point}/MyDrive not accessible")
            return False

    except ImportError:
        print("✗ Error: google.colab module not available.")
        print("  This script is designed to run in Google Colab environment.")
        print()
        print("  If you're running locally, you can:")
        print("  1. Use rclone to mount Google Drive")
        print("  2. Use the Google Drive desktop app")
        print("  3. Upload files manually")
        return False

    except Exception as e:
        print(f"✗ Error mounting Google Drive: {e}")
        return False


def verify_drive_access(mount_point: str = "/content/drive") -> bool:
    """
    Verify that Google Drive is accessible and we can list contents.

    Args:
        mount_point: The mount point to verify.

    Returns:
        True if Drive is accessible, False otherwise.
    """
    mydrive_path = Path(mount_point) / "MyDrive"

    if not mydrive_path.exists():
        print(f"✗ {mydrive_path} does not exist")
        return False

    try:
        # Try to list contents to verify access
        contents = list(mydrive_path.iterdir())
        print(f"✓ Google Drive accessible ({len(contents)} items in MyDrive)")
        return True
    except PermissionError:
        print("✗ Permission denied accessing Google Drive")
        return False
    except Exception as e:
        print(f"✗ Error accessing Google Drive: {e}")
        return False


def parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Mount Google Drive for use in Colab terminal.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    python scripts/mount_drive.py
    python scripts/mount_drive.py --mount-point /content/drive
    python scripts/mount_drive.py --force-remount
    python scripts/mount_drive.py --check-only
        """,
    )

    parser.add_argument(
        "--mount-point",
        type=str,
        default="/content/drive",
        help="Mount point for Google Drive (default: /content/drive)",
    )

    parser.add_argument(
        "--force-remount",
        action="store_true",
        help="Force remount even if already mounted",
    )

    parser.add_argument(
        "--check-only",
        action="store_true",
        help="Only check if Drive is mounted, don't attempt to mount",
    )

    parser.add_argument(
        "--quiet",
        "-q",
        action="store_true",
        help="Suppress non-error output",
    )

    return parser.parse_args()


def main() -> int:
    """
    Main entry point for the script.

    Returns:
        Exit code (0 for success, non-zero for failure).
    """
    args = parse_args()

    # Check-only mode
    if args.check_only:
        if is_drive_mounted(args.mount_point):
            if not args.quiet:
                print(f"✓ Google Drive is mounted at {args.mount_point}")
            return 0
        else:
            if not args.quiet:
                print(f"✗ Google Drive is not mounted at {args.mount_point}")
            return 1

    # Check if we're in Colab environment
    if not os.path.exists("/content"):
        if not args.quiet:
            print("⚠ Warning: /content directory not found.")
            print("  This script is designed for Google Colab.")
            print()

    # Attempt to mount
    success = mount_google_drive(
        mount_point=args.mount_point,
        force_remount=args.force_remount,
    )

    if not success:
        return 1

    # Verify access
    if not verify_drive_access(args.mount_point):
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
