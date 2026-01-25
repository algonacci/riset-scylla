from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

cluster = Cluster(["127.0.0.1"])
session = cluster.connect()

rows = session.execute("SELECT release_version FROM system.local")
for row in rows:
    print("Scylla version:", row.release_version)

cluster.shutdown()
