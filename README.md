# prod-databricks-retailq-project

Key Design Decisions
1. Separate Managed Ingestion Pipelines
Create two managed ingestion pipelines (not SDP):
	• Salesforce pipeline - Full or incremental sync to bronze
	• PostgreSQL pipeline - CDC or snapshot to bronze
These are configured via the Lakeflow Connect API (not Python/SQL code).
2. Single SDP Pipeline for Transformations
One Spark Declarative Pipeline handles:
	• Auto Loader ingestion from blob storage → bronze
	• Bronze → Silver transformations (cleaning, deduplication, conforming)
Silver → Gold aggregations (business metrics, star schema)