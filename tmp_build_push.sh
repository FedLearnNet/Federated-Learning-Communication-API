docker build . -t ghcr.io/fedlearnnet/federated-learning-communication-api/controller:tmptag --push
docker build . --file Dockerfile.relay -t ghcr.io/fedlearnnet/federated-learning-communication-api/controller-relay:tmptag --push
