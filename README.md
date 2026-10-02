# Live Food Delivery – Real-Time Data Pipeline

Simulated food delivery platform streaming live order events end to end.

**Pipeline:** Python generator → PostgreSQL → Kafka → FastAPI (consumer + WebSocket) → live dashboard

## Run locally
docker compose up -d
docker exec -i ldp-postgres psql -U app -d shop < sql/01_schema.sql
docker exec -i ldp-postgres psql -U app -d shop < sql/02_seed.sql
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
python generator.py            # terminal 1
uvicorn app:app --port 8000    # terminal 2
Open http://localhost:8000

## Roadmap
- [x] MVP: live dashboard
- [ ] Deploy to live.ravidata.com
- [ ] Debezium CDC (replace dual writes)
- [ ] Stream processing: windows, late events
- [ ] Schema registry + dead-letter queue
- [ ] Star schema + dbt batch layer
- [ ] Orchestration + data quality
- [ ] Monitoring
