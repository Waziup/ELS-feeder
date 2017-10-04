
echo "Docker building, testing, and pushing"

docker build -t waziup/feeder2 .
docker run  -it waziup/feeder2
docker push waziup/feeder2

echo "K8S deployment"
./deploy-k8s.sh