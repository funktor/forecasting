#!/bin/bash
VALID_ARGS=$(getopt -o r:i:d: --long registry:,image:,dockerpath: -- "$@")
if [[ $? -ne 0 ]]; then
    exit 1;
fi

eval set -- "$VALID_ARGS"
while [ : ]; do
  case "$1" in
    -r | --registry)
        REGISTRY=$2
        shift 2
        ;;
    -i | --image)
        IMAGE=$2
        shift 2
        ;;
    -d | --dockerpath)
        DOCKERFILE_PATH=$2
        shift 2
        ;;
    --) shift; 
        break 
        ;;
  esac
done

sudo service docker start

docker build -f $DOCKERFILE_PATH -t $IMAGE .
docker tag $IMAGE $REGISTRY/$IMAGE
docker push $REGISTRY/$IMAGE