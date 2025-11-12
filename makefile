worker:
	source .venv/bin/activate &&  celery -A src.celery_task worker -l INFO --pool=gevent -c 2
run:
	source .venv/bin/activate &&  python -m src.main

flower:
	source .venv/bin/activate && celery -A celery_task flower --port=8008 
