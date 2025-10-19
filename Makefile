init_docker: ## Levanta servicios en segundo plano de Docker
	docker compose up -d

clear_docker: 
	docker-compose down -v

clean:
	uv run main.py --yaml ./pipelines/limpieza/raw_api_data.yaml
	uv run main.py --yaml ./pipelines/limpieza/raw_campaigns.yaml
	uv run main.py --yaml ./pipelines/limpieza/raw_client_file.yaml
	uv run main.py --yaml ./pipelines/limpieza/raw_holidays.yaml
	bash ./scripts/limpieza_messages.sh
	uv run main.py --yaml ./pipelines/limpieza/messages_cleared.yaml

transformations:
	uv run main.py --yaml ./pipelines/transformacion/enriched_mesages.yaml
	uv run main.py --yaml ./pipelines/transformacion/enriched_campaigns.yaml
	uv run main.py --yaml ./pipelines/transformacion/enriched_clients.yaml
	uv run main.py --yaml ./pipelines/transformacion/enriched_holidays.yaml
	uv run main.py --yaml ./pipelines/transformacion/campaign_performance.yaml
	uv run main.py --yaml ./pipelines/transformacion/campaign_performance_full.yaml

carga_sql: # Se requiere que se inicie Docker para hacer esta carga. 
	uv run main.py --yaml ./pipelines/carga/sql/carga_holidays_table.yaml
	uv run main.py --yaml ./pipelines/carga/sql/carga_campaigns_table.yaml
	bash ./scripts/carga_tablas_pesadas.sh
	uv run main.py --yaml ./pipelines/carga/sql/carga_agg_campaign_table.yaml

carga_csv: 
	uv run main.py --yaml ./pipelines/carga/csv/holidays_table_csv.yaml
	uv run main.py --yaml ./pipelines/carga/csv/campaigns_table_csv.yaml
	uv run main.py --yaml ./pipelines/carga/csv/client_table_csv.yaml
	uv run main.py --yaml ./pipelines/carga/csv/messages_table_csv.yaml
	uv run main.py --yaml ./pipelines/carga/csv/agg_campaign_table_csv.yaml

validations: 
	uv run main.py --yaml ./pipelines/validaciones/validacion_campaign.yaml --ver-cli
	uv run main.py --yaml ./pipelines/validaciones/validacion_clients_gold.yaml
	uv run main.py --yaml ./pipelines/validaciones/validacion_messages_gold.yaml

validations_git:
	@echo "Ejecutando validaciones..."
	set -x
	uv run main.py --yaml ./pipelines/validaciones/validacion_campaign.yaml || (echo "Error ejecutando validaciones"; exit 1)

tests: 
	uv run -m pytest

lint:
	uv run ruff check .