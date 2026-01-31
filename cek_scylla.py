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

def print_table(title, query):
    print(f"\n=== {title} ===")
    try:
        rows = session.execute(query)
        # Ambil nama kolom dari metadata result set
        colnames = rows.column_names
        
        count = 0
        for row in rows:
            count += 1
            print(f"\n--- Log #{count} ---")
            for col in colnames:
                val = getattr(row, col)
                print(f"{col.ljust(15)}: {val}")
        
        if count == 0:
            print("No data found.")
    except Exception as e:
        print(f"Error: {e}")

# Query tables in the specified keyspace
print_table("ENTERPRISE ACTIVITY LOGS (AUDIT TRAIL)", f"SELECT * FROM {keyspace}.activity_logs ALLOW FILTERING")
print_table("CHAT MESSAGES", f"SELECT * FROM {keyspace}.messages")
print_table("USER EVENTS", f"SELECT * FROM {keyspace}.user_events")

cluster.shutdown()
