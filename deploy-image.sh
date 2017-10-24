echo "Docker building, testing, and pushing"
version=V1_REL-teta
git tag ${version}
docker build -t waziup/feeder .
sleep 3
docker tag waziup/feeder waziup/feeder:${version}
docker images
sleep 3
docker push waziup/feeder:${version}
