"""Unit test: send_email SMTP path with a fake SMTP server (no network).

Run: python tests/test_smtp_unit.py
"""
import importlib.util
import os
import sqlite3
import sys
import tempfile

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

SENT = []  # captures (from, to, message) delivered to the fake server


class FakeSMTP:
    def __init__(self, host, port, timeout=None):
        self.host, self.port = host, port
        assert host and port

    def starttls(self):
        pass

    def login(self, user, password):
        assert user == "bot@example.com", f"unexpected user {user!r}"
        assert password == "dummy-pass"

    def send_message(self, msg):
        SENT.append((msg["From"], msg["To"], msg.get_content()))

    def quit(self):
        pass


def main():
    tmp = tempfile.mkdtemp()
    db_path = os.path.join(tmp, "unit.db")
    os.environ["LEAD_DISPATCH_DB"] = db_path
    os.environ["SMTP_HOST"] = "localhost"
    os.environ["SMTP_PORT"] = "2525"
    os.environ["SMTP_USER"] = "bot@example.com"
    os.environ["SMTP_PASS"] = "dummy-pass"
    os.environ["SMTP_MIN_GAP_SECONDS"] = "0"
    # Make sure no .env in cwd interferes
    os.environ.pop("SMTP_CONFIG", None)

    spec = importlib.util.spec_from_file_location(
        "lds", os.path.join(REPO, "lead_dispatch_system.py"))
    lds = importlib.util.module_from_spec(spec)
    sys.modules["lds"] = lds
    spec.loader.exec_module(lds)

    # Patch smtplib used inside send_email
    import smtplib
    real_smtp = smtplib.SMTP
    smtplib.SMTP = FakeSMTP
    try:
        # Seed a lead with a valid email
        conn = sqlite3.connect(db_path)
        conn.execute(
            "INSERT INTO leads (name, category, email, created_at, status)"
            " VALUES ('Acme Co', 'plumbing', 'buyer@acme.test', "
            " datetime('now'), 'new')")
        conn.commit()
        conn.close()

        ok = lds.outreach.send_email(
            1, "intro_english", city="Mumbai", service="plumbing",
            sender="TestBot")
        assert ok, "send_email returned False"
        assert len(SENT) == 1, f"expected 1 send, got {len(SENT)}"
        frm, to, body = SENT[0]
        assert to == "buyer@acme.test", f"wrong recipient: {to}"
        assert "Acme Co" in body, "template not rendered with lead name"

        # Verify DB logging: message row + contact_count bump
        conn = sqlite3.connect(db_path)
        n = conn.execute("SELECT COUNT(*) FROM messages").fetchone()[0]
        cc = conn.execute(
            "SELECT contact_count, status FROM leads WHERE id=1").fetchone()
        conn.close()
        assert n == 1, f"expected 1 logged message, got {n}"
        assert cc[0] == 1 and cc[1] == "contacted", f"lead not updated: {cc}"

        print("test_smtp_send: PASS (1 email delivered, logged, lead updated)")
    finally:
        smtplib.SMTP = real_smtp


if __name__ == "__main__":
    main()
