SET controllerName=fc-controller
SET controllerLabel=fc-controller-label

for /F %%i in ('docker ps -a -q --filter "label=%controllerLabel%"') do docker rm -f %%i

docker kill %controllerName%
docker rm %controllerName%
docker network rm %controllerName%-internal

docker volume prune --force --filter "label=%controllerLabel%"
docker network prune --force --filter "label=%controllerLabel%"
