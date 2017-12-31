#!/bin/bash

proj=`pwd`
src="${proj}/src"
venv="${proj}/venv"
dist="${proj}/dist"

rm -rf "${venv}"
virtualenv -p python3 "${venv}"
source "${venv}/bin/activate"
pip install -r "${proj}/requirements.txt"

rm -rf "${dist}"
mkdir "${dist}"
cp "${proj}/graph-wars-fce132407f00.json" "${dist}/creds.json"

pushd "${venv}/lib/python3.5/site-packages"
cp -r * "${dist}"
popd

pushd "${src}"
cp -r * "${dist}"
popd

pushd "${dist}"
rm "${proj}/dist.zip"
zip -r "${proj}/dist.zip" .
popd
