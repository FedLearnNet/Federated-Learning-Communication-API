docker build . -t gitlab.cosy.bio:5050/cosybio/federated-learning/federated_db/feature-cloud-controller/controller:tmptag --push
docker build . --file Dockerfile.relay -t gitlab.cosy.bio:5050/cosybio/federated-learning/federated_db/feature-cloud-controller/controller-relay:tmptag --push
