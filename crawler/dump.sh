#!/bin/bash

sdate="$0"
edate="$1"
until [[ $sdate == $edate ]]; do
    echo "${sdate}"
    docker run --rm \
        -e AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID}" \
        -e AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY}" \
        fonapi-crawler python3 crawler.py --start-date "${sdate}" &>> crawler.log
    sdate=$(date -v "+1d" -jf "%d.%m.%Y" "${sdate}" "+%d.%m.%Y")
done
