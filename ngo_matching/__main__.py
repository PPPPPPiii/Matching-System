import sys


def _fail_for_unsupported_python() -> None:
    if sys.version_info < (3, 9):
        raise SystemExit(
            "Unsupported Python version. This project requires Python 3.9+.\n"
            "Use commands like: python3 -m venv .venv && "
            "source .venv/bin/activate && python3 -m pip install -r requirements.txt"
        )


if __name__ == "__main__":
    _fail_for_unsupported_python()
    from .cli import main

    raise SystemExit(main())
