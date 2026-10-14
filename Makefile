export PYTHONPATH := src

.PHONY: install eval demo dashboard up down test lint

install:    ## install dependencies
	pip install -r requirements-dev.txt
eval:       ## run 8 simulated hours for 100 vehicles as fast as possible and score detection
	python -m fleetstream.run_local --vehicles 100 --minutes 480
demo:       ## live demo without Kafka: paced stream + dashboard on :8501
	python -m fleetstream.run_local --vehicles 60 --minutes 720 --speedup 30 & streamlit run dashboard.py
dashboard:
	streamlit run dashboard.py
up:         ## full stack: Redpanda (Kafka) + producer + detector + dashboard (:8501) + console (:8080)
	docker compose up --build
down:
	docker compose down -v
test:
	pytest -q
lint:
	ruff check .
