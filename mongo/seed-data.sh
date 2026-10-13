#!/bin/bash
# Seeds datavault.events with sample documents so they distribute across shards.
# Run after init-cluster.sh has completed successfully.
set -e

docker exec mongo-router mongosh --port 27017 --quiet --eval '
const db = db.getSiblingDB("datavault");
const bulk = [];
for (let i = 0; i < 50000; i++) {
  bulk.push({
    user_id: Math.floor(Math.random() * 100000),
    event_type: ["click", "view", "purchase", "signup"][Math.floor(Math.random() * 4)],
    ts: new Date(),
    payload: { session: Math.random().toString(36).slice(2) }
  });
  if (bulk.length === 1000) {
    db.events.insertMany(bulk);
    bulk.length = 0;
  }
}
if (bulk.length) db.events.insertMany(bulk);
print("Inserted 50000 sample events.");
'

echo "==> Seed complete. Chunk distribution:"
docker exec mongo-router mongosh --port 27017 --quiet --eval '
db.getSiblingDB("datavault").events.getShardDistribution()
'
