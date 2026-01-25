from cassandra.cluster import Cluster

# Connect to ScyllaDB
cluster = Cluster(["127.0.0.1"])
session = cluster.connect()

print("=== Checking Keyspace: chat_app ===")

# Cek User Events (Join/Left)
print("\n[User Events (Last 10)]")
try:
    # Note: CQL doesn't support generic LIMIT without partition key in some versions, 
    # but for simple verification allowing filtering or just selecting simple works for small tables.
    # Adding ALLOW FILTERING just in case, though ideally we verify partitions.
    rows = session.execute("SELECT * FROM chat_app.user_events LIMIT 10")
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
    rows = session.execute("SELECT * FROM chat_app.messages LIMIT 10")
    count = 0
    for row in rows:
        count += 1
        print(f"User: {row.user_id} | Msg: {row.content} | Time: {row.timestamp}")
    if count == 0:
        print("No messages found yet.")
except Exception as e:
    print(f"Error querying messages: {e}")

cluster.shutdown()
