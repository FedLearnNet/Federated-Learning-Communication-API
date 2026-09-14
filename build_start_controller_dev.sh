docker build -t feddb_fc_controller -f ./Dockerfile.dev .
mkdir -p data

docker kill fc-controller > /dev/null 2>&1 
docker rm fc-controller > /dev/null 2>&1

docker run \
 -p 8002:8002 \
 --name fc-controller \
 -v "/var/run/docker.sock:/var/run/docker.sock" \
 --mount "type=bind,source=$(pwd)/data,target=/data" \
 fc_controller "--internal-root=/data"
