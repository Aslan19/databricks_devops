import sys, pytest

# avoid __pycache__ issues in Repos
sys.dont_write_bytecode = True

exit_code = pytest.main([
    "tests",
    "-v",
    "-p", "no:cacheprovider"
])

if exit_code != 0:
    raise Exception(f"Tests failed with exit code {exit_code}")
