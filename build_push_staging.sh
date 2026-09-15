set -e
echo "This will push staging images to the registry. Please do not use this on a macbook as the staging server is linux based! Are you sure you want to proceed? (y/n)"
read answer
if [ "$answer" != "y" ]; then
    echo "Aborting."
    exit 1
fi

# relay
docker build -t ghcr.io/fedlearnnet/federated-learning-communication-api/controller-relay:staging -f Dockerfile.relay . --push
# controller
docker build -t ghcr.io/fedlearnnet/federated-learning-communication-api/controller:staging -f Dockerfile . --push
