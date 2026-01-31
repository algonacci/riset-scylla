from cassandra.cluster import Cluster
from scylla_helper import get_scylla_host

cluster = Cluster([get_scylla_host()], port=9042, connect_timeout=30)
session = cluster.connect()

rows = session.execute("SELECT release_version FROM system.local")
for row in rows:
    print("Scylla version:", row.release_version)

cluster.shutdown()
