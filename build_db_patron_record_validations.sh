#!/bin/bash
cd "$(dirname "$0")"

while true; do
    ./venv/bin/jupyter nbconvert \
        --to html \
        --execute build_db_patron_record_validations.ipynb \
        --output build_db_patron_record_validations.html
done
