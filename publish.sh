set -x

# Build
docker build . -t stephben/fetch-with-prefect:latest

# Test
docker run -ti stephben/fetch-with-prefect py.test tests/

# Publish to hub.docker.com
docker login --username stephben
docker push stephben/fetch-with-prefect:latest