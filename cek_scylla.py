from cassandra.cluster import Cluster

# Connect to ScyllaDB
cluster = Cluster(["127.0.0.1"])
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

# Keluarkan semua data dari activity_logs
print_table("ENTERPRISE ACTIVITY LOGS (AUDIT TRAIL)", "SELECT * FROM chat_app.activity_logs ALLOW FILTERING")

# Keluarkan semua data dari messages
print_table("CHAT MESSAGES", "SELECT * FROM chat_app.messages")

# Keluarkan semua data dari user_events
print_table("USER EVENTS", "SELECT * FROM chat_app.user_events")

cluster.shutdown()
