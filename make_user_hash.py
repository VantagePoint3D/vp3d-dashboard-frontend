"""Generate a password hash for DASHBOARD_USERS.

Usage:  python3 make_user_hash.py
Prompts for a password, prints the pbkdf2 hash string to paste into the
DASHBOARD_USERS env var on Railway, e.g.:
  [{"email":"brett@vantagepoint3d.com","password_hash":"<paste>","role":"owner"}]
"""
import base64, getpass, hashlib, os

ITERATIONS = 600_000

def make_hash(password: str) -> str:
    salt = os.urandom(16)
    dk = hashlib.pbkdf2_hmac("sha256", password.encode(), salt, ITERATIONS)
    return f"pbkdf2${ITERATIONS}${base64.b64encode(salt).decode()}${base64.b64encode(dk).decode()}"

if __name__ == "__main__":
    pw = getpass.getpass("Password to hash: ")
    pw2 = getpass.getpass("Repeat: ")
    if pw != pw2:
        raise SystemExit("Passwords do not match.")
    if len(pw) < 8:
        raise SystemExit("Use at least 8 characters.")
    print(make_hash(pw))
