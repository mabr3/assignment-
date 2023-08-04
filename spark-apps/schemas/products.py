from pyspark.sql.types import StringType, StructType, FloatType
class Products:
    schema = (StructType()
            .add("product_id", StringType())
            .add("product_name", StringType())
            .add("price", FloatType())
    )

    def get_schema(self):
        return self.schema