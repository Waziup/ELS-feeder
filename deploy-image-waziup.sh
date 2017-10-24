echo "Docker building, testing, and pushing"
version=waziup
git tag ${version}
docker build -t waziup/feeder2 .
sleep 3
docker tag waziup/feeder2 waziup/feeder2:${version}
docker images
sleep 3
docker push waziup/feeder2:${version}
