cd /home/GdoczAI
source /home/GdoczAI/gd_env/bin/activate
exec uvicorn src.services.ocr_pipeline.ocr_server_app:app --port 3545
