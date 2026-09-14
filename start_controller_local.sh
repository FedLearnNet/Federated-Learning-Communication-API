mkdir -p data

docker kill fc-controller > /dev/null 2>&1
docker rm fc-controller > /dev/null 2>&1
docker network rm fc-controller-internal > /dev/null 2>&1
docker build -t featurecloud.ai/controller:local .

docker run \
 -d \
 -p 8000:8000 \
 --name fc-controller \
 -v "/var/run/docker.sock:/var/run/docker.sock" \
 --mount "type=bind,source=$(pwd)/data,target=/data" \
 featurecloud.ai/controller:local "--host-root=$(pwd)/data" "--internal-root=/data" "--controller-name=fc-controller"

