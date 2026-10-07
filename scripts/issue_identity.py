"""Trusted-backend development helper. Never bundle IDENTITY_SECRET in a client."""
import argparse
import os
from duplex_voice.control.identity import issue_identity


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--tenant", required=True)
    p.add_argument("--subject", required=True)
    args = p.parse_args()
    secret = os.environ.get("IDENTITY_SECRET", "")
    if len(secret) < 32:
        p.error("set IDENTITY_SECRET to a random 32+ character secret in the trusted backend")
    print(issue_identity(secret, args.tenant, args.subject,
                         issuer=os.environ.get("IDENTITY_ISSUER", "audio-pipeline-app"),
                         cell=os.environ.get("CELL_ID", "default")))


if __name__ == "__main__":
    main()
