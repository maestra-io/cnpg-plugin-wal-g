"""A pgoutput consumer that confirms flush the way Debezium does in the incident
of cloudnative-pg/cloudnative-pg#11715 (used by hack/e2e/switchover-e2e.sh).

- It confirms flush only up to just below the last COMMIT it received, never the
  trailing non-transactional WAL (the CHECKPOINT the instance manager issues
  before a switchover). A logical walsender exits at shutdown only once its
  client has confirmed everything it sent, so the old primary's shutdown
  checkpoint waits for it.
- On SIGUSR1 (sent by the test when it starts a switchover) it keeps answering
  for CDC_HOLD_SECONDS, then stops reading the connection, so the walsender is
  reaped by wal_sender_timeout after the physical ones, as in the incident.
- On any connection error it reconnects to PGHOST (the -rw service), creating
  the failover slot if the new primary does not have it.

Connection settings come from the libpq environment (PGHOST, PGUSER, ...).
"""

import os
import select
import signal
import sys
import time

import psycopg2
import psycopg2.extras

SLOT = os.environ.get("CDC_SLOT", "cdc_slot")
PUBLICATION = os.environ.get("CDC_PUBLICATION", "cdc_pub")
HOLD_SECONDS = float(os.environ.get("CDC_HOLD_SECONDS", "4"))
APP_NAME = "cdc-e2e"

freeze_at = None  # monotonic time after which the consumer stops reading


def on_usr1(_signum, _frame):
    global freeze_at
    freeze_at = time.monotonic() + HOLD_SECONDS
    log(f"SIGUSR1: answering for {HOLD_SECONDS}s more, then going silent")


def log(msg):
    print(time.strftime("%H:%M:%S", time.gmtime()), msg, flush=True)


def ensure_slot():
    conn = psycopg2.connect(application_name=APP_NAME + "-admin", connect_timeout=5)
    try:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute("select pg_is_in_recovery()")
            if cur.fetchone()[0]:
                raise RuntimeError("connected to a standby")
            cur.execute("select 1 from pg_replication_slots where slot_name = %s", (SLOT,))
            if cur.fetchone() is None:
                cur.execute(
                    "select pg_create_logical_replication_slot(%s, 'pgoutput', false, false, true)", (SLOT,)
                )
                log(f"created failover slot {SLOT}")
    finally:
        conn.close()


def stream():
    global freeze_at
    freeze_at = None
    ensure_slot()
    conn = psycopg2.connect(
        application_name=APP_NAME,
        connect_timeout=5,
        connection_factory=psycopg2.extras.LogicalReplicationConnection,
    )
    cur = conn.cursor()
    cur.start_replication(
        slot_name=SLOT, decode=False, status_interval=10,
        options={"proto_version": "1", "publication_names": PUBLICATION},
    )
    log("streaming")
    flush = 0
    last_feedback = 0.0
    while True:
        if freeze_at is not None and time.monotonic() >= freeze_at:
            # Silent: no reads, no replies. The walsender times out; the next
            # read after that fails and we reconnect.
            time.sleep(1)
            if time.monotonic() - freeze_at > 20:
                raise RuntimeError("went silent 20s ago, reconnecting")
            continue
        msg = cur.read_message()
        if msg is not None:
            # pgoutput: 'C' is a COMMIT. Confirm just below it: never the WAL after
            # the last commit, and never exactly the last message (psycopg2 would
            # then forward the flush position to the keepalive's walEnd by itself).
            if msg.payload[:1] == b"C" and msg.data_start > 0:
                flush = msg.data_start - 1
                cur.send_feedback(flush_lsn=flush, force=True)
                last_feedback = time.monotonic()
            continue
        if time.monotonic() - last_feedback >= 1:
            # Answer keepalives with the unchanged position, like a busy client.
            cur.send_feedback(flush_lsn=flush, force=True)
            last_feedback = time.monotonic()
        select.select([cur], [], [], 0.5)


def main():
    signal.signal(signal.SIGUSR1, on_usr1)
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(0))
    while True:
        try:
            stream()
        except Exception as e:  # noqa: BLE001 - reconnect on anything
            log(f"connection lost: {e.__class__.__name__}: {str(e).strip()}")
            time.sleep(1)


if __name__ == "__main__":
    main()
