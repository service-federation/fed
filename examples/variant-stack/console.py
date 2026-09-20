"""An attachable catalog console. Detaching leaves this process running."""

import json
import os
from pathlib import Path
from urllib.error import URLError
from urllib.request import urlopen

UNHEALTHY_MARKER = Path(".fed/catalog-unhealthy")
HELP = "Commands: catalog, fail, recover, help, quit. Detach: Ctrl+P, Ctrl+Q."


def show_catalog():
    try:
        with urlopen(os.environ["CATALOG_URL"] + "/catalog", timeout=2) as response:
            print(json.dumps(json.load(response), indent=2))
    except (URLError, TimeoutError, OSError) as error:
        print(f"Catalog unavailable: {error}")


def main():
    print(f"Catalog console. {HELP}", flush=True)
    while True:
        try:
            command = input("catalog> ").strip()
        except EOFError:
            break

        if command == "quit":
            break
        elif command == "catalog":
            show_catalog()
        elif command == "fail":
            UNHEALTHY_MARKER.parent.mkdir(exist_ok=True)
            UNHEALTHY_MARKER.touch()
            print("Catalog health disabled; watch fed status.")
        elif command == "recover":
            UNHEALTHY_MARKER.unlink(missing_ok=True)
            print("Catalog health restored.")
        elif command:
            print(HELP)


if __name__ == "__main__":
    main()
