import hypersync
import polars
import asyncio
import holoviews as hv
from hypersync import DataType, TransactionField, BlockField

DATA_PATH = "data/gas_usage"

async def get_data():
    client = hypersync.HypersyncClient("https://linea.hypersync.xyz")

    await client.create_parquet_folder(
        hypersync.Query(
            from_block=0,
            transactions=[hypersync.TransactionSelection()],
            include_all_blocks=True,
            field_selection=hypersync.FieldSelection(
                transaction=[
                    TransactionField.BLOCK_NUMBER,
                    TransactionField.GAS_USED,
                ],
                block=[
                    BlockField.NUMBER,
                    BlockField.TIMESTAMP,
                ]
            )
        ),
        hypersync.ParquetConfig(
            path=DATA_PATH,
            hex_output=True,
            retry=True,
            column_mapping=hypersync.ColumnMapping(
                block={
                    BlockField.TIMESTAMP: DataType.INT64,
                },
                transaction={
                    TransactionField.GAS_USED: DataType.FLOAT64,
                }
            ),
        )
    )

def plot_contract_deployment_density():
    transactions = polars.read_parquet(
        DATA_PATH + "/transactions.parquet"
    )

    data = polars.read_parquet(
        DATA_PATH + "/blocks.parquet"
    ).select(
        "*",
        polars.col("timestamp").mul(int(1e6)).cast(polars.Datetime).alias("timestamp_datetime")
    ).join(
        transactions,
        left_on=polars.col("number"),
        right_on=polars.col("block_number"),
    ).sort(
        "timestamp_datetime",
    ).group_by_dynamic(
        index_column=polars.col("timestamp_datetime"),
        every="24h",
        label='left',
    ).agg(
        polars.col("gas_used").sum().alias("total_gas_used"),
    )

    print(data)

    renderer = hv.renderer('bokeh')
    graph = data.plot.bar(x="timestamp_datetime", y="total_gas_used")
    renderer.save(graph, DATA_PATH + "/graph")

def main():
    asyncio.run(get_data())
    plot_contract_deployment_density()

main()
