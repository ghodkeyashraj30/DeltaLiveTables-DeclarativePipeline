import dlt


#customers_expectation
customer_rules = {
    "rule1" : "customer_id is NOT NULL",
    "rule2" : "customer_name is NOT NULL"
}

#Ingesting customers
@dlt.table(
    name = "customers_stg"
)

@dlt.expect_all_or_drop(customer_rules)

def customers_stg():
    df = spark.readStream.table("dltyash.source.customers")
    return df