#!/bin/bash

DIR_PATH="../cpp/buildSQLite"


if [ -d "$DIR_PATH" ]; then
  echo "Directory $DIR_PATH already exists. Removing it..."
  rm -rf "$DIR_PATH"
fi

mkdir -p ../cpp/buildSQLite
cp -r ../refs ../cpp/buildSQLite
cd ../cpp/buildSQLite/
cmake ../ -DUSE_SGX=OFF -DCMAKE_PREFIX_PATH=$HOME/.local
make -j 4