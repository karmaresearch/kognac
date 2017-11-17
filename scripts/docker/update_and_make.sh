#!/bin/sh
cd /app/kognac
git pull
cd build
cmake ..
make
cd ../build_debug
cmake ..
make
cd ..
