SET basePath=%cd%
SET controllerName=feddb-fc-controller

mkdir data

docker kill %controllerName%
docker rm %controllerName%

docker run ^
-d ^
-p 8002:8002 ^
--name %controllerName% ^
-v "/var/run/docker.sock:/var/run/docker.sock" ^
--mount "type=bind,source=%basePath%/data,target=/data" ^
feddb_fc_controller "--internal-root=/data"
