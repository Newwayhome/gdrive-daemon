#!/usr/bin/env python3
"""
Interactive Google Drive User OAuth 2.0 Token Generator
Generates token.json for gdrive-daemon using desktop client credentials.
"""

import json
import os
import sys
from pathlib import Path

try:
    from google_auth_oauthlib.flow import InstalledAppFlow
    from google.oauth2.credentials import Credentials
except ImportError:
    print("❌ Missing required dependencies!")
    print("Please install them using:")
    print("    pip install -r requirements.txt")
    sys.exit(1)

SCOPES = ["https://www.googleapis.com/auth/drive"]

def find_client_secrets_file():
    candidates = [
        "client_secret.json",
        "client_secrets.json",
        "credentials.json",
        "oauth.json",
    ]
    for c in candidates:
        p = Path(c)
        if p.exists():
            try:
                with open(p, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    if "installed" in data or "web" in data:
                        return p
            except Exception:
                continue
    return None

def main():
    print("=" * 70)
    print("  GDrive Daemon - OAuth Token Generator (token.json)")
    print("=" * 70)

    client_secrets_path = find_client_secrets_file()

    if not client_secrets_path:
        print("\n🔍 No OAuth client secret JSON file found in this directory.")
        print("\nYou can either:")
        print("  1. Download the OAuth Client JSON from Google Cloud Console and rename it to 'client_secret.json'")
        print("  2. Enter your Client ID and Client Secret manually below\n")
        
        choice = input("Enter manually? (y/n) [default: y]: ").strip().lower()
        if choice not in ("n", "no"):
            client_id = input("Enter Client ID: ").strip()
            client_secret = input("Enter Client Secret: ").strip()
            if not client_id or not client_secret:
                print("❌ Client ID and Client Secret cannot be empty.")
                sys.exit(1)
            
            client_config = {
                "installed": {
                    "client_id": client_id,
                    "client_secret": client_secret,
                    "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                    "token_uri": "https://oauth2.googleapis.com/token",
                    "redirect_uris": ["http://localhost"]
                }
            }
            flow = InstalledAppFlow.from_client_config(client_config, SCOPES)
        else:
            print("\nPlease place 'client_secret.json' in this directory and re-run this script.")
            sys.exit(1)
    else:
        print(f"🔑 Found OAuth client credentials: {client_secrets_path}")
        flow = InstalledAppFlow.from_client_secrets_file(str(client_secrets_path), SCOPES)

    print("\n🌐 Opening browser for Google Drive authorization...")
    try:
        creds = flow.run_local_server(port=0)
    except Exception as e:
        print(f"❌ Local server authentication failed ({e}). Attempting console flow...")
        creds = flow.run_console()

    token_data = {
        "token": creds.token,
        "refresh_token": creds.refresh_token,
        "token_uri": creds.token_uri,
        "client_id": creds.client_id,
        "client_secret": creds.client_secret,
        "scopes": creds.scopes or SCOPES
    }

    out_file = Path("token.json")
    with open(out_file, "w", encoding="utf-8") as f:
        json.dump(token_data, f, indent=2)

    print("\n" + "=" * 70)
    print(f"✅ Success! Token saved to: {out_file.resolve()}")
    print("You can now build and run gdrive-daemon:")
    print("    go build -o gdrive-daemon")
    print("    ./gdrive-daemon")
    print("=" * 70)

if __name__ == "__main__":
    main()
