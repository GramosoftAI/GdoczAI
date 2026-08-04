#!/bin/bash
cd /home/GdoczAI
source /home/GdoczAI/mineru_env/bin/activate
exec python -m src.services.smtp_fetch.start_smtp_fetcher --port 4545

