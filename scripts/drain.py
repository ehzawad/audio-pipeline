"""Local operator hook: stop new admission, await bounded active-call drain."""
import json
import os
import urllib.request


def main():
    token = os.environ.get("APP_TOKEN", "")
    if not token:
        raise SystemExit("APP_TOKEN is required; never put it on the command line")
    request = urllib.request.Request("http://127.0.0.1:8000/admin/drain", data=b"",
                                     headers={"Authorization": "Bearer " + token}, method="POST")
    timeout = float(os.environ.get("DRAIN_SECONDS", "30")) + 15
    with urllib.request.urlopen(request, timeout=timeout) as response:
        print(json.loads(response.read()))


if __name__ == "__main__":
    main()
