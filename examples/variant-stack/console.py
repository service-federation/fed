"""An attachable catalog console. Detaching leaves this process running."""

import json
import os
from pathlib import Path
from urllib.error import URLError
from urllib.request import urlopen

switch = Path(".fed/catalog-unhealthy")
print("Catalog console. Commands: catalog, fail, recover, help, quit", flush=True)
while True:
    try:
        command = input("catalog> ").strip()
    except EOFError:
        break
    if command == "quit":
        break
    if command == "fail":
        switch.parent.mkdir(exist_ok=True)
        switch.touch()
        print("Catalog health disabled; watch fed status.")
    elif command == "recover":
        switch.unlink(missing_ok=True)
        print("Catalog health restored.")
    elif command == "catalog":
        try:
            with urlopen(os.environ["CATALOG_URL"] + "/catalog", timeout=2) as response:
                print(json.dumps(json.load(response), indent=2))
        except (URLError, TimeoutError, OSError) as error:
            print(f"Catalog unavailable: {error}")
    elif command:
        print("Commands: catalog, fail, recover, help, quit. Detach: Ctrl+P, Ctrl+Q.")
