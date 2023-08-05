from pyspark.sql.types import StringType, StructType
class Customers:
    schema = (StructType()
            .add("customer_id", StringType())
            .add("company_name", StringType())
            .add("specialized_industries", StringType())
    )

    def get_schema(self):
        return self.schema

class Companies:
    schema = (StructType()
            .add("company_name", StringType())
            .add("industry", StringType())
    )

    def get_schema(self):
        return self.schema