mkdir -p data

docker kill feddb-fc-controller > /dev/null 2>&1
docker rm feddb-fc-controller > /dev/null 2>&1

docker run \
 -d \
 -p 8002:8002 \
 --name feddb-fc-controller \
 -v "/var/run/docker.sock:/var/run/docker.sock" \
 --mount "type=bind,source=$(pwd)/data,target=/data" \
 feddb_fc_controller "--internal-root=/data"
