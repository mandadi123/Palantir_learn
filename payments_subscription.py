from transforms.api import transform_df, Input, Output
from pyspark.sql import functions as F 
from pyspark.sql.functions import when ,col


@transform_df(
    Output("/CUSTOMERCARE-d37351/[Source]Customer/data/clean/payments_subscription"),
    source_df1=Input("/CUSTOMERCARE-d37351/[Source]Customer/uploads/payments_enriched_par"),
    source_df2=Input("ri.foundry.main.dataset.b9dbea32-0caa-4673-82e2-75f6657f3745"),
)
def compute(source_df1,source_df2):
    df1 = source_df1.toDF(*[c.lower() for c in source_df1.columns])
    df2 = source_df2.toDF(*[c.lower() for c in source_df2.columns])
    df1=df1.filter(F.col("Status")=='Pending')
    
    df1 = df1.withColumn("payment_category",
         when(col("total_paid") <= 100, "Bronze_payment")
         .when(col("total_paid") .between(101,200), "Silver_payment")
         .when(col("total_paid") .between(201,300), "Gold_payment")
         .when(col("total_paid") > 300, "Diamond_payment")
         .otherwise("total_paid")
    )
    df2=df2.select("subscription_id","start_date","end_date","contract_type")
    df_join=df1.join(df2,df1["subscription_id"] == df2["subscription_id"],"inner")
    df_join=df_join.select(df1.payment_id,df1.subscription_id,df1.payment_date,df1.amount,df1.tax,df1.total_paid,df1.payment_method,df1.status,df1.invoice_number,df1.late_fee,df1.discount,df1.payment_channel,
    df1.processed_by,df2.contract_type,df2.start_date,df2.end_date)
    return df_join

