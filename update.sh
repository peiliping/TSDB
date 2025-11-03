#!/bin/bash

wget https://github.com/peiliping/TSDB/archive/refs/heads/main.zip
unzip main.zip
rm -rf main.zip
mv TSDB-main/*.lua ./
rm -rf TSDB-main