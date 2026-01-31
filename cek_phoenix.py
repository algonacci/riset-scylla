import os
import argparse
from cassandra.cluster import Cluster
from scylla_helper import get_scylla_host

# Parse arguments or use env var
parser = argparse.ArgumentParser(description="Query ScyllaDB keyspace")
parser.add_argument("--keyspace", "-k", default=os.getenv("SCYLLA_KEYSPACE", ""),
                    help="Keyspace name (default: SCYLLA_KEYSPACE env or prompt)")
args = parser.parse_args()

keyspace = args.keyspace
if not keyspace:
    keyspace = input("Masukkan keyspace name: ").strip()

if not keyspace:
    print("Error: Keyspace required!")
    exit(1)

# Connect to ScyllaDB
cluster = Cluster([get_scylla_host()], port=9042, connect_timeout=30)
session = cluster.connect()

print(f"=== Checking Keyspace: {keyspace} ===")

# Cek User Events (Join/Left)
print("\n[User Events (Last 10)]")
try:
    rows = session.execute(f"SELECT * FROM {keyspace}.user_events LIMIT 10")
    count = 0
    for row in rows:
        count += 1
        print(f"User: {row.user_id} | Event: {row.event_type} | Time: {row.timestamp}")
    if count == 0:
        print("No events found yet.")
except Exception as e:
    print(f"Error querying user_events: {e}")

# Cek Messages
print("\n[Messages (Last 10)]")
try:
    rows = session.execute(f"SELECT * FROM {keyspace}.messages LIMIT 10")
    count = 0
    for row in rows:
        count += 1
        print(f"User: {row.user_id} | Msg: {row.content} | Time: {row.timestamp}")
    if count == 0:
        print("No messages found yet.")
except Exception as e:
    print(f"Error querying messages: {e}")

cluster.shutdown()
