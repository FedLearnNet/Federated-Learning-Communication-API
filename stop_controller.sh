docker rm -f $(docker ps -a -q --filter "label=fc-controller-label")

docker kill fc-controller > /dev/null 2>&1
docker rm fc-controller > /dev/null 2>&1
docker network rm fc-controller-internal > /dev/null 2>&1

docker volume prune --force --filter "label=fc-controller-label"
docker network prune --force --filter "label=fc-controller-label"
