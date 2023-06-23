import datahub.emitter.mce_builder as builder
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.com.linkedin.pegasus2avro.dataset import (
    DatasetLineageType,
    FineGrainedLineage,
    FineGrainedLineageDownstreamType,
    FineGrainedLineageUpstreamType,
    Upstream,
    UpstreamLineage,
)

from sqlalchemy import create_engine
from sqlalchemy.sql import text


def datasetUrn(tbl, enviroment, platform):
    return builder.make_dataset_urn(platform, tbl, enviroment)


def fldUrn(tbl, col, enviroment, platform):# el esquema de un dataset son las columnas de una tabla
    return builder.make_schema_field_urn(datasetUrn(tbl, enviroment, platform), col)


def emitTableLineages(con):
    statement = text(""" SELECT DISTINCT 
                               plataforma_origen, esquema_origen, tabla_origen,
                               plataforma_destino, esquema_destino, tabla_destino
                           FROM etl_tool.dv_entities_mapping 
                           ORDER BY
                               plataforma_origen, esquema_origen, tabla_origen,
                               plataforma_destino, esquema_destino, tabla_destino""")
    
    mapping_table = con.execute(statement)
    # 1 emit for each lineage relationship
    for row in mapping_table:
        # Construct a lineage object.
        lineage_mce = builder.make_lineage_mce(
            [
                builder.make_dataset_urn(row[0], row[1]+'.'+row[2]),  # Upstream f string
            ],
            builder.make_dataset_urn(row[3], row[4]+'.'+row[5]),  # Downstream
        )
        # Emit metadata!
        emitter.emit_mce(lineage_mce)


def emitColLineages(colLineages, upstream_platform, upstream_dataset, downstream_platform, downstream_dataset):
    upstream = Upstream(dataset=datasetUrn(upstream_dataset, "PROD", upstream_platform), type=DatasetLineageType.COPY)

    fieldLineages = UpstreamLineage(
        upstreams=[upstream], fineGrainedLineages=colLineages
    )

    lineageMcp = MetadataChangeProposalWrapper(
        entityUrn=datasetUrn(downstream_dataset, "PROD", downstream_platform),
        aspect=fieldLineages,
    )
    
    # Emit metadata!
    emitter.emit_mcp(lineageMcp)


def getColLineages(db_name, db_user, db_pass, db_host, db_port):
    # Connect to the database
    db_string = f"postgresql://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}"
    engine = create_engine(db_string)
   
    with engine.connect() as con:
        emitTableLineages(con)
        statement = text(""" SELECT 
                                plataforma_origen, esquema_origen, tabla_origen, columna_origen,
                                plataforma_destino, esquema_destino, tabla_destino, columna_destino
                            FROM etl_tool.dv_entities_mapping 
                            ORDER BY
                                plataforma_origen, esquema_origen, tabla_origen,
                                plataforma_destino, esquema_destino, tabla_destino,
                                columna_origen, columna_destino""")
        mapping_table = con.execute(statement)

        fineGrainedLineages = []
        prev_up_table = []
        prev_down_table = []
        for row in mapping_table:
            upstream_table = [row[0], row[1], row[2]]
            downstream_table = [row[4], row[5], row[6]]

            if not prev_up_table or (prev_up_table == upstream_table and prev_down_table == downstream_table):
                finegrainedlineage = FineGrainedLineage(
                    upstreamType=FineGrainedLineageUpstreamType.FIELD_SET,
                    upstreams=[fldUrn(row[1]+'.'+row[2], row[3], 'PROD', row[0])],
                    downstreamType=FineGrainedLineageDownstreamType.FIELD,
                    downstreams=[fldUrn(row[5]+'.'+row[6], row[7], 'PROD', row[4])]
                )
                fineGrainedLineages.append(finegrainedlineage)
            else:
                # Emit metadata!
                emitColLineages(fineGrainedLineages
                                , prev_up_table[0], prev_up_table[1]+'.'+prev_up_table[2]
                                , prev_down_table[0], prev_down_table[1]+'.'+prev_down_table[2])

                fineGrainedLineages = []

                finegrainedlineage = FineGrainedLineage(
                    upstreamType=FineGrainedLineageUpstreamType.FIELD_SET,
                    upstreams=[fldUrn(row[1]+'.'+row[2], row[3], 'PROD', row[0])],
                    downstreamType=FineGrainedLineageDownstreamType.FIELD,
                    downstreams=[fldUrn(row[5]+'.'+row[6], row[7], 'PROD', row[4])],
                )
                fineGrainedLineages.append(finegrainedlineage)
                
            prev_up_table = [row[0], row[1], row[2]]
            prev_down_table = [row[4], row[5], row[6]]

        emitColLineages(fineGrainedLineages
                        , prev_up_table[0], prev_up_table[1]+'.'+prev_up_table[2]
                        , prev_down_table[0], prev_down_table[1]+'.'+prev_down_table[2])


db_name = 'data_lake'
db_user = 'psql_user'
db_pass = 'datahub'
db_host = 'localhost'
db_port = '54320'

# Create an emitter to the GMS REST API.
emitter = DatahubRestEmitter("http://localhost:8080")

getColLineages(db_name, db_user, db_pass, db_host, db_port)