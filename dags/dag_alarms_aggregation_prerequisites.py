# Nome da DAG: dag_alarms_aggregation_prerequisites
# Owner / responsável: CoE
# Descrição do objetivo da DAG: Crea/modifica las tablas en Postgres para guardar las configuraciones de thresholds prod
# Usa Druid?: No
# Principais tabelas / consultas Druid acessadas: 
# Frequência de execução (schedule): Demanda
# Dag Activo?: No
# Autor: CoE
# Data de modificação: 2025-05-26
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'CoE',
    'start_date': days_ago(1),
}

with DAG('dag_alarms_aggregation_prerequisites',
         default_args=default_args,
         schedule_interval=None,  # Este DAG se ejecutará manualmente
         tags=["alarms", "aggregation"],
         catchup=False) as dag:
    
    create_schema_panda_task = PostgresOperator(
        task_id = 'create_schema_panda_task',
        postgres_conn_id = 'postgres_prd',  
        sql = "CREATE SCHEMA IF NOT EXISTS panda;"
    )

    create_table_threshold_task = PostgresOperator(
        task_id = 'create_table_threshold_task',
        postgres_conn_id = 'postgres_prd',  
        # sql = """
        #     CREATE TABLE IF NOT EXISTS panda.threshold (
        #         node_id varchar(50) NULL,
        #         node_name varchar(100) NULL,
        #         "dataAggregationTime" varchar(50) NULL,
        #         "dataAggregationTimeGranularity" varchar(50) NULL,
        #         "dataAggregationFunction" varchar(50) NULL,
        #         "dataAggregationPercentileValue" int4 NULL,
        #         "spatialAggregation" varchar(50) NULL,
        #         "spatialAggregationFunction" varchar(50) NULL,
        #         "spatialAggregationPercentileValue" float4 NULL,
        #         "id_spatialLocationTypeAggregation" varchar(50) NULL,
        #         "spatialLocationTypeAggregation" varchar(100) NULL,
        #         "id_spatialManagedObjectAggregation" varchar(50) NULL,
        #         "spatialManagedObjectAggregation" varchar(100) NULL,
        #         "managedObject_id" varchar(50) NULL,
        #         "managedObject_name" varchar(100) NULL,
        #         "dataTableName" varchar(50) NULL,
        #         expression_id varchar(50) NULL,
        #         "ngExclusion" varchar NULL,
        #         "id_ngExclusion" varchar NULL,
        #         "ngInclusion" varchar NULL,
        #         "id_ngInclusion" varchar NULL,
        #         "id_ngInclusion_SG" varchar NULL,
        #         "typeAssociation" varchar NULL,
        #         "equipmentExclusion" varchar NULL,
        #         "id_equipmentExclusion" varchar NULL,
        #         "equipmentInclusion" varchar NULL,
        #         "id_equipmentInclusion" varchar NULL,
        #         kpi_id varchar(50) NULL,
        #         kpi_name varchar(100) NULL,
        #         "kpi_defaultTimeGranularity" varchar(50) NULL,
        #         "kpi_isRate" bool NULL,
        #         kpi_unit varchar(10) NULL,
        #         kpi_hourly varchar(10) NULL,
        #         kpi_daily varchar(10) NULL,
        #         kpi_weekly varchar(10) NULL,
        #         kpi_monthly varchar(10) NULL,
        #         kpi_yearly varchar(10) NULL,
        #         kpi_fifteenDays varchar(10) NULL,
        #         kpi_quarterly varchar(10) NULL,
        #         kpi_semesterly varchar(10) NULL,
        #         execution_date varchar(20) NULL,
        #         load_status varchar(20) NULL,
        #         "timeZone" varchar NULL
        #     ); 
        # """

        # sql = """
        #         ALTER TABLE panda.threshold
        #         ADD COLUMN algorithm varchar(60),
        #  		  ADD COLUMN algorithmComparisonValue integer,
        #  	      ADD COLUMN algorithmComparisonType varchar(30),
        #  		  ADD COLUMN intervalDays integer,
        #  		  ADD COLUMN times integer,
        #         ADD COLUMN hourlyPercentile float4,
        #         ADD COLUMN dailyPercentile float4,
        #         ADD COLUMN weeklyPercentile float4,
        #         ADD COLUMN monthlyPercentile float4,
        #         ADD COLUMN yearlyPercentile float4,
        #         ADD COLUMN fifteenDaysPercentile float4,      
        #         ADD COLUMN quarterlyPercentile float4,
        #         ADD COLUMN semesterlyPercentile float4;
        #     """

        # sql="""
        #     ALTER TABLE panda.threshold
        #     ALTER COLUMN algorithmComparisonValue TYPE VARCHAR(10),
        #     ALTER COLUMN intervalDays TYPE VARCHAR(10),
        #     ALTER COLUMN times TYPE VARCHAR(10);
        # """

        sql = """
                ALTER TABLE panda.threshold
                ADD COLUMN effectiveDays varchar NULL,
                ADD COLUMN optionsDays varchar NULL,
                ADD COLUMN effectiveHours varchar NULL;
            """
    )

    create_table_control_load_data_task = PostgresOperator(
        task_id = 'create_table_control_load_data__task',
        postgres_conn_id = 'postgres_prd', 
        sql = """
            CREATE TABLE IF NOT EXISTS panda.control_load_data (
            kpi_id varchar NOT NULL,
            kpi_name varchar NULL,
            managedobject_id varchar NOT NULL,
            managedobject varchar NULL,
            granularityinterval varchar NOT NULL,
            networkgroups varchar NULL,
            equipments varchar NULL,
            delay int4 NULL,
            completness int4 NULL,
            records int4 NULL,
            status varchar NULL,
            start_date timestamp NOT NULL,
            end_date timestamp NOT NULL,
            aggregation_time varchar NULL,
            load_datatime timestamp NOT NULL,
            pending int4 NULL,
            id_process serial4 NOT NULL,
            log varchar NULL,
            threshold_id varchar NULL,
            update_data timestamp DEFAULT CURRENT_TIMESTAMP NOT NULL
        );
        """

        # sql = """
        #         ALTER TABLE panda.control_load_data 
        #         ALTER COLUMN managedobject DROP NOT NULL;
        #     """
    )

    create_schema_panda_task >> create_table_threshold_task >> create_table_control_load_data_task 
