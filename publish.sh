set -x
set -e
# Build
docker build . -t stephben/datafetch:latest

# Test
#docker run -ti stephben/datafetch py.test tests/

# Publish to hub.docker.com
docker login --username stephben
docker push stephben/datafetch:latest