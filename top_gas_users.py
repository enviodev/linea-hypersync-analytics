import hypersync
import polars
import asyncio

DATA_PATH = "data/top_gas_users"

async def get_data():
    client = hypersync.HypersyncClient("https://linea.hypersync.xyz")

    await client.create_parquet_folder(
        hypersync.Query(
            from_block=0,
            transactions=[hypersync.TransactionSelection()],
            field_selection=hypersync.FieldSelection(
                transaction=[
                    hypersync.TransactionField.FROM,
                    hypersync.TransactionField.BLOCK_NUMBER,
                    hypersync.TransactionField.GAS_USED,
                    hypersync.TransactionField.EFFECTIVE_GAS_PRICE,
                ]
            )
        ),
        hypersync.ParquetConfig(
            path=DATA_PATH,
            hex_output=True,
            retry=True,
            column_mapping=hypersync.ColumnMapping(
                transaction={
                    hypersync.TransactionField.GAS_USED: hypersync.DataType.FLOAT64,
                    hypersync.TransactionField.EFFECTIVE_GAS_PRICE: hypersync.DataType.FLOAT64,
                }
            ),
        )
    )

def find_top_wallets():
    data = polars.read_parquet(
        DATA_PATH + "/transactions.parquet"
    ).select(
        polars.col("*")
    ).group_by(
        polars.col("from")
    ).agg(
        polars.col(hypersync.TransactionField.GAS_USED).mul(
            polars.col(hypersync.TransactionField.EFFECTIVE_GAS_PRICE)
        ).sum().alias("total_gas_cost")
    ).sort(
        polars.col("total_gas_cost"),
        descending=True
    ).limit(10)

    polars.Config.set_ascii_tables()
    polars.Config.set_tbl_width_chars(100)
    polars.Config.set_fmt_str_lengths(50)

    print(data)

def main():
    asyncio.run(get_data())
    find_top_wallets()

main()
