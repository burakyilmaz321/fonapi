#!/bin/bash

# Install apt and pip requirements
sudo apt-get update
sudo apt-get install -y --no-install-recommends python3-pip firefox

# gecko
wget https://github.com/mozilla/geckodriver/releases/download/v0.27.0/geckodriver-v0.27.0-linux32.tar.gz
tar -xvzf geckodriver*
chmod +x geckodriver
sudo mv geckodriver /usr/local/bin/

# crawler
wget https://raw.githubusercontent.com/burakyilmaz321/fonapi/master/crawler/crawler.py
wget https://raw.githubusercontent.com/burakyilmaz321/fonapi/master/crawler/requirements.txt

python3 -m pip install pip setuptools --upgrade
python3 -m pip install -r requirements.txt
