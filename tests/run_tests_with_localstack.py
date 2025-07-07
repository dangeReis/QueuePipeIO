#!/usr/bin/env python3
"""
Test runner that automatically starts LocalStack if S3 tests are included.

This script can be used by VSCode or run manually to ensure LocalStack
is available when running S3 integration tests.
"""

import subprocess
import sys
import time
import unittest
import argparse


def is_localstack_running():
    """Check if LocalStack is running and healthy."""
    try:
        result = subprocess.run(
            ["curl", "-f", "http://localhost:4566/_localstack/health"],
            capture_output=True,
            timeout=2
        )
        return result.returncode == 0
    except Exception:
        return False


def start_localstack():
    """Start LocalStack using docker-compose."""
    print("Starting LocalStack...")
    try:
        subprocess.run(["make", "localstack-start"], check=True)
        print("✅ LocalStack started successfully")
        return True
    except subprocess.CalledProcessError:
        print("❌ Failed to start LocalStack")
        return False


def stop_localstack():
    """Stop LocalStack."""
    print("Stopping LocalStack...")
    try:
        subprocess.run(["make", "localstack-stop"], check=True)
        print("✅ LocalStack stopped")
    except subprocess.CalledProcessError:
        print("❌ Failed to stop LocalStack")


def run_tests(pattern="test*.py", verbosity=2, stop_on_error=False):
    """Run the test suite."""
    # Discover and run tests
    loader = unittest.TestLoader()
    start_dir = "tests"
    suite = loader.discover(start_dir, pattern=pattern)
    
    runner = unittest.TextTestRunner(
        verbosity=verbosity,
        failfast=stop_on_error
    )
    
    result = runner.run(suite)
    return result.wasSuccessful()


def main():
    parser = argparse.ArgumentParser(
        description="Run QueuePipeIO tests with automatic LocalStack management"
    )
    parser.add_argument(
        "--pattern", 
        default="test*.py",
        help="Test file pattern (default: test*.py)"
    )
    parser.add_argument(
        "--no-s3",
        action="store_true",
        help="Skip S3 tests (don't start LocalStack)"
    )
    parser.add_argument(
        "--keep-localstack",
        action="store_true",
        help="Don't stop LocalStack after tests"
    )
    parser.add_argument(
        "-v", "--verbose",
        action="count",
        default=2,
        help="Increase test verbosity"
    )
    parser.add_argument(
        "-f", "--failfast",
        action="store_true",
        help="Stop on first test failure"
    )
    
    args = parser.parse_args()
    
    # Check if we need S3 tests
    needs_s3 = not args.no_s3 and (
        "s3" in args.pattern or 
        args.pattern == "test*.py"
    )
    
    localstack_started = False
    
    try:
        # Start LocalStack if needed
        if needs_s3:
            if not is_localstack_running():
                if start_localstack():
                    localstack_started = True
                    # Give LocalStack time to fully initialize
                    time.sleep(2)
                else:
                    print("⚠️  Continuing without S3 tests")
            else:
                print("✅ LocalStack is already running")
        
        # Run tests
        print(f"\n{'='*60}")
        print(f"Running tests matching pattern: {args.pattern}")
        print(f"{'='*60}\n")
        
        success = run_tests(
            pattern=args.pattern,
            verbosity=args.verbose,
            stop_on_error=args.failfast
        )
        
        return 0 if success else 1
        
    finally:
        # Clean up LocalStack if we started it
        if localstack_started and not args.keep_localstack:
            stop_localstack()


if __name__ == "__main__":
    sys.exit(main())