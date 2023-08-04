from pyspark.sql.types import StringType, StructType
class Industries:
    schema = (StructType()
            .add("industry", StringType())
        )

    def get_schema(self):
        return self.schema