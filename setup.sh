#!/bin/bash -ex

path=$(readlink -f "${BASH_SOURCE[0]}")
DIR_PATH=$(dirname "$path")
BASE_DIR=$(dirname "$DIR_PATH")

#todo - pip install
cd $(python3 -c "import sysconfig; print(sysconfig.get_path('purelib'))")

echo $DIR_PATH > rec_sys-internal.pth
echo $DIR_PATH/utils >> rec_sys-internal.pth
echo $DIR_PATH/recommendation_service >> rec_sys-internal.pth
echo $DIR_PATH/grpc >> rec_sys-internal.pth
echo $DIR_PATH/config >> rec_sys-internal.pth
echo $BASE_DIR >> rec_sys-internal.pth

export PYTHONPATH="$DIR_PATH/utils:$DIR_PATH/recommendation_service:$DIR_PATH/grpc:$DIR_PATH/config:$DIR_PATH/utils:$BASE_DIR:$PYTHONPATH"
echo "PYTHONPATH set to: $PYTHONPATH"