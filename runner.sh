#!/bin/bash

apt-get update && \
apt-get install python3-venv wget git -y && \
git clone https://github.com/marinade-finance/solana-snapshot-finder && \
cd solana-snapshot-finder && \
python3 -m venv venv && \
source ./venv/bin/activate && \
pip3 install -r requirements.txt
python3 snapshot-finder.py --max_latency 400 --min_download_speed 1 --snapshot_path /data/
/usr/local/bin/solana-snapshot-etl "$(ls /data/*.tar.zst)" --sqlite-out /data/snapshot.db
exec "$@"