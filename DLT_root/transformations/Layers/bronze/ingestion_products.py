import dlt

#products expectation
products_rules = {
    "rule1" : "product_id IS NOT NULL",
    "rule2" : "price >= 0"
}
#Ingesting products
@dlt.table(
    name = "products_stg"
)

@dlt.expect_all_or_drop(products_rules)

def products_stg():
    df = spark.readStream.table("dltyash.source.products")
    return df