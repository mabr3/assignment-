from pyspark.sql.types import  StructType, StructField, StringType, ArrayType, DoubleType, FloatType, IntegerType, TimestampType


class Order:
    schema = (StructType()
         .add("order_id", StringType(), nullable=False)
         .add("customer_id", StringType(),nullable=False)
         .add("order_lines", ArrayType(
             StructType([StructField('price', FloatType(),nullable=False), 
                         StructField('product_id', StringType(),nullable=False), 
                         StructField('volume', IntegerType(),nullable=False)]
                         )
                        ), nullable=False
            )
         .add("amount", FloatType(), nullable=False)
         .add("timestamp", TimestampType(), nullable=False)
        )
    def get_schema(self):
        return self.schema

class OrderLine:
    schema = (StructType()
        .add("product_id", StringType(), False)
        .add("volume", IntegerType(), False)
        .add("price", FloatType(), False)
    )
    def get_schema(self):
        return self.schema