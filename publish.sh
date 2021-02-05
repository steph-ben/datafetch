set -x
set -e
# Build
docker build . -t stephben/datafetch:latest

# Test
docker run --name datatest --rm -ti stephben/datafetch /bin/bash -c "pip install pytest ; py.test tests/"

# Publish to hub.docker.com
cat .docker_passwd | docker login --username stephben --password-stdin
docker push stephben/datafetch:latest
