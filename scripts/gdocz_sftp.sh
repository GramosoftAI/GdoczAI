#!/bin/bash

cd /home/GdoczAI
source /home/GdoczAI/mineru_env/bin/activate

exec python -m src.services.sftp_fetch.start_sftp_fetch --port 3535

