#!/usr/bin/env python3
"""
Test runner script for agentcore
"""

import subprocess
import sys


def run_tests():
    """Run all tests"""
    print("Running agentcore tests...")

    # Run pytest
    result = subprocess.run(
        [sys.executable, "-m", "pytest", "tests/", "-v", "--tb=short", "--asyncio-mode=auto"],
        check=False,
        capture_output=False,
    )

    return result.returncode


def run_linter():
    """Run linter checks"""
    print("Running linter checks...")

    # Run ruff
    result = subprocess.run(
        [sys.executable, "-m", "ruff", "check", "."], check=False, capture_output=False
    )

    return result.returncode


def main():
    """Main test runner"""
    print("🔧 AgentCore Test Suite")
    print("=" * 50)

    # Run linter first
    print("\n📏 Running linter...")
    lint_result = run_linter()

    if lint_result != 0:
        print("❌ Linter checks failed!")
        return lint_result

    print("✅ Linter checks passed!")

    # Run tests
    print("\n🧪 Running tests...")
    test_result = run_tests()

    if test_result == 0:
        print("✅ All tests passed!")
        return 0
    print("❌ Some tests failed!")
    return test_result


if __name__ == "__main__":
    sys.exit(main())
