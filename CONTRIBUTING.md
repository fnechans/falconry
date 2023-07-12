# Contributing to falconry

Any contributions or feedback are welcome, please use github issues to report any problems.

## Development

I use flake8 and mypy to check for syntax errors, style and correct typing. These are checked in the CI but you can run them locally.

Syntax + undefined names:

    python3 -m  flake8 . --count --select=E9,F63,F7,F82 --show-source

Any other problems, those do not cause pipeline to fail, only show warning:

    python3 -m  flake8 . --count --statistics

And finally mypy for type errors:

    python3 -m mypy
