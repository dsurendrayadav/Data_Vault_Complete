#!/bin/bash
# Run this AFTER `docker compose up -d` and after all mongo containers are healthy.
# It wires the config server + 3 shards + mongos into one working sharded cluster.
set -e

echo "==> Initiating config server replica set..."
docker exec mongo-cfg mongosh --port 27019 --quiet --eval '
rs.initiate({
  _id: "cfgrs",
  configsvr: true,
  members: [{ _id: 0, host: "mongo-cfg:27019" }]
})
'

echo "==> Initiating shard1 replica set..."
docker exec mongo-shard1 mongosh --port 27018 --quiet --eval '
rs.initiate({ _id: "shard1rs", members: [{ _id: 0, host: "mongo-shard1:27018" }] })
'

echo "==> Initiating shard2 replica set..."
docker exec mongo-shard2 mongosh --port 27018 --quiet --eval '
rs.initiate({ _id: "shard2rs", members: [{ _id: 0, host: "mongo-shard2:27018" }] })
'

echo "==> Initiating shard3 replica set..."
docker exec mongo-shard3 mongosh --port 27018 --quiet --eval '
rs.initiate({ _id: "shard3rs", members: [{ _id: 0, host: "mongo-shard3:27018" }] })
'

echo "==> Waiting 10s for replica sets to elect primaries..."
sleep 10

echo "==> Adding shards to the router and enabling sharding..."
docker exec mongo-router mongosh --port 27017 --quiet --eval '
sh.addShard("shard1rs/mongo-shard1:27018");
sh.addShard("shard2rs/mongo-shard2:27018");
sh.addShard("shard3rs/mongo-shard3:27018");

sh.enableSharding("datavault");

db.getSiblingDB("datavault").events.createIndex({ user_id: 1 });
sh.shardCollection("datavault.events", { user_id: 1 });
'

echo "==> Cluster ready. Checking status:"
docker exec mongo-router mongosh --port 27017 --quiet --eval 'sh.status()'
