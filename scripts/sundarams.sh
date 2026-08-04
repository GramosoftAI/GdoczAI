cd /home/GdoczAI
source /home/GdoczAI/mineru_env/bin/activate
exec uvicorn src.services.sundarams.sundarams_ocr_server_app:app --port 4433
