#!/bin/bash

# Activate virtualenv
source /home/autotiler@ad.dream.upd.edu.ph/.virtualenvs/automation-python2/bin/activate

# Change dir
pushd /home/autotiler@ad.dream.upd.edu.ph/lipad-data-tools/automation

# LOG="workers_in_salad.log"
LOG="logs/workers.log"
#PYTHON_CMD="python -u workers.py"

# @TODO
# add to rc.local or cron tab using lock file

python -u workers.py
