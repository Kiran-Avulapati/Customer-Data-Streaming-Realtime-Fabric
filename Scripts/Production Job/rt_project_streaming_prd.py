from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from delta.tables import DeltaTable
spark=SparkSession.builder.getOrCreate()

df_bronze=(
    spark
    .readStream
    .format('delta')
    .table("bronze.API_Raw_Data")
)

df_valid=df_bronze.filter(trim(col('results'))!='[]')

df_normalized=df_valid.withColumn('results',
                when(
                    trim(col('results')).startswith("["),
                    trim(col('results'))).
                otherwise(
                    concat(lit('['),trim(col('results')),
                    lit(']')
                    )
                )
            )

df_silver=df_normalized.select(
        get_json_object(col('results'),"$[0].gender").alias('Gender'),
        get_json_object(col('results'),"$[0].name.title").alias('Title'),
        get_json_object(col('results'),"$[0].name.first").alias('First'),
        get_json_object(col('results'),"$[0].name.last").alias('Last'),
        concat(get_json_object(col('results'),"$[0].location.street.number"),lit(', '),get_json_object(col('results'),"$[0].location.street.name")).alias('Street_Info'),
        get_json_object(col('results'),"$[0].location.city").alias('City'),
        get_json_object(col('results'),"$[0].location.state").alias('State'),
        get_json_object(col('results'),"$[0].location.country").alias('Country'),
        get_json_object(col('results'),"$[0].location.postcode").alias('Postcode'),
        get_json_object(col('results'),"$[0].location.coordinates.latitude").alias('Latitude'),
        get_json_object(col('results'),"$[0].location.coordinates.longitude").alias('Longitude'),
        get_json_object(col('results'),"$[0].location.timezone.offset").alias('TimeZone_Offset'),
        get_json_object(col('results'),"$[0].location.timezone.description").alias('TimeZone_Description'),
        get_json_object(col('results'),"$[0].email").alias('Email'),
        get_json_object(col('results'),"$[0].login.uuid").alias('userid'),
        get_json_object(col('results'),"$[0].login.username").alias('User_Name'),
        get_json_object(col('results'),"$[0].login.password").alias('Password'),
        get_json_object(col('results'),"$[0].login.salt").alias('Salt'),
        get_json_object(col('results'),"$[0].login.md5").alias('MD5'),
        get_json_object(col('results'),"$[0].login.sha1").alias('SHA1'),
        get_json_object(col('results'),"$[0].login.sha256").alias('SHA256'),
        get_json_object(col('results'),"$[0].dob.date").alias('Birth_Date'),
        get_json_object(col('results'),"$[0].dob.age").alias('Age'),
        get_json_object(col('results'),"$[0].registered.date").alias('Registered_Date'),
        get_json_object(col('results'),"$[0].registered.age").alias('Registered_Age'),
        get_json_object(col('results'),"$[0].phone").alias('Phone_Number'),
        get_json_object(col('results'),"$[0].cell").alias('Cell_Number'),
        get_json_object(col('results'),"$[0].id.name").alias('ID_Name'),
        get_json_object(col('results'),"$[0].id.value").alias('ID_Value'),
        get_json_object(col('results'),"$[0].picture.large").alias('Picture_Large'),
        get_json_object(col('results'),"$[0].picture.medium").alias('Picture_Medium'),
        get_json_object(col('results'),"$[0].picture.thumbnail").alias('Picture_thumbnail'),
        get_json_object(col('results'),"$[0].nat").alias('NAT'),
        col('injestion_timestamp').alias('Injestion_Timestamp'),
        current_timestamp().alias("Processing_Timestamp")

        )

query=(df_silver.writeStream
                .format('delta')
                .outputMode("append")
                .option('checkpointLocation',
                        'Files/checkpoint/api_silver')
                .toTable("Silver.API_Silver_Data")
    )

silver_stream_df=(spark.readStream.format('delta')
                .option('startingVersion','latest')
                .table('Silver.api_silver_data'))

gold_stream_df=silver_stream_df.select(
    col('userid').alias('user_id'),
    col('Gender').alias('gender'),
    col('Title').alias('title'),
    col("First").alias('first_name'),
    col('Last').alias('last_name'),
    concat_ws(' ',trim(col('first')),trim(col('last'))).alias('full_name'),
    col('Email').alias('email'),
    col('City').alias('city'),
    col('State').alias('state'),
    col('Country').alias('country'),
    col('Postcode').alias('postcode'),
    col("Latitude").cast('double').alias('latitude'),
    col("Longitude").cast('double').alias('longitude'),
    col('NAT').alias('nationality'),
    col('Age').alias('age'),
    col('Birth_Date').alias('birth_date'),
    col('Registered_Date').alias('registered_date'),
    col('Registered_Age').cast('int').alias('registered_age'),
    col('Phone_Number').alias('phone_number'),
    col('Cell_Number').alias('cell_number'),
    col('Injestion_Timestamp').alias("injection_timestamp"),
    col('Processing_Timestamp').alias('processing_timestamp')
)

def upsert_gold_customers(batch_df,batch_id):
    latest_batch_df=batch_df.selectExpr(
        "*",
        "row_number() over(partition by user_id order by injection_timestamp desc) as rn").filter('rn==1').drop('rn')
    gold_table=DeltaTable.forName(spark,'gold.gold_customers')
    (gold_table.alias('target').merge(latest_batch_df.alias('source'),'target.user_id=source.user_id')
    .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute())
gold_query=(
    gold_stream_df.
    writeStream.
    foreachBatch(upsert_gold_customers).
    option('checkpointLocation','Files/checkpoints/silver_to_gold_customers').start())
print('Silver --> Gold Stream started successfully')

quality_stream_df=(
    gold_stream_df
    .withColumn(
        "profile_completeness_score",
        (
            when(col('first_name').isNotNull(),1).otherwise(0)
            +
            when(col('last_name').isNotNull(),1).otherwise(0)
            +
            when(col('email').isNotNull(),1).otherwise(0)
            +
            when(col('phone_number').isNotNull(),1).otherwise(0)
            +
            when(col('country').isNotNull(),1).otherwise(0)
            +
            when(col('city').isNotNull(),1).otherwise(0)
            +
            when(col('age').isNotNull(),1).otherwise(0)
        )
    )
)

quality_stream_df=(
    quality_stream_df.withColumn(
        "profile_quality",
        when(col('profile_completeness_score')==7,"Complete").
        when(col('profile_completeness_score')>=5,"Mostly Complete").
        when(col('profile_completeness_score')>=3,"Partially Complete").otherwise('Poor')
    )
)

def upsert_quality(batch_df,batch_id):
    window_spec=Window.partitionBy('user_id').orderBy(col('injection_timestamp').desc())
    latest_batch_df=(
        batch_df.withColumn('rn',row_number().over(window_spec)).filter(col('rn')==1).drop('rn')
    )
    gold=DeltaTable.forName(spark,'gold.gold_data_quality')
    (gold.alias('t').merge(latest_batch_df.alias('s'),'t.user_id=s.user_id')
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute())

quality_query=(
    quality_stream_df.writeStream
    .foreachBatch(upsert_quality)
    .option('checkpointLocation','Files/checkpoints/quality')
    .start()
)

metrics_stream_df=(
    gold_stream_df
    .withColumn('processing_latency_seconds',
    unix_timestamp('processing_timestamp')
    -
    unix_timestamp('injection_timestamp'))
)

def upsert_metrics(batch_df,batch_id):
    window_spec=Window.partitionBy('user_id').orderBy(col('injection_timestamp').desc())
    latest_batch_df=(
        batch_df.withColumn('rn',row_number().over(window_spec)).filter(col('rn')==1).drop('rn')
    )
    table=DeltaTable.forName(
        spark,'gold.gold_pipeline_metrics'
    )
    (table.alias('target')
    .merge(
        latest_batch_df.alias('source'),
        'target.user_id=source.user_id'
    )
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()
    )

metrics_query=(
    metrics_stream_df.writeStream
    .foreachBatch(upsert_metrics)
    .option(
        'checkpointLocation',
        'Files/checkpoints/metrics'
    )
    .start()
)

demographics_stream_df=(
            spark.readStream.format('delta').
            table('silver.api_silver_data')
            .select(
                col('userid').alias('user_id'),
                col('Gender').alias('gender'),
                col('Age').alias('age'),
                col('NAT').alias('nationality'),
                col("Injestion_Timestamp").alias('Injection_Timestamp')
            ))

def update_demographics(batch_df,batch_id):
    window_spec=Window.partitionBy('user_id').orderBy(col('injection_timestamp').desc())
    new_df=(
    batch_df.withColumn('rn',row_number().over(window_spec)).filter(col('rn')==1).drop('rn')
    .withColumn(
    'age_group',
    when(col('age')<18,'Under 18').
    when(col('age')<=25,'18-25').
    when(col('age')<=35,'26-35').
    when(col('age')<=50,'36-50').
    otherwise('51+'))
    .select('user_id','gender','age_group','nationality'))
    state=DeltaTable.forName(spark,'gold.gold_demographic_state')
    changes=(
            new_df.alias('n').join(state.toDF().alias('s'),'user_id','left')
            .select(
                col('s.gender').alias('old_gender'),
                col('s.age_group').alias('old_age_group'),
                col('s.nationality').alias('old_nationality'),
                col('n.gender').alias('new_gender'),
                col('n.age_group').alias('new_age_group'),
                col('n.nationality').alias('new_nationality')
                )
            )

    old_changes=(changes
            .filter(col('old_gender').isNotNull())
            .select(
                col('old_gender').alias('gender'),
                col('old_age_group').alias('age_group'),
                col('old_nationality').alias('nationality'),
                lit(-1).alias('delta')
            ))

    new_changes=(changes
            .filter(col('new_gender').isNotNull())
            .select(
                col('new_gender').alias('gender'),
                col('new_age_group').alias('age_group'),
                col('new_nationality').alias('nationality'),
                lit(1).alias('delta')
            ))

    deltas=(
        old_changes.
        unionByName(new_changes)
        .groupBy('gender','age_group','nationality')
        .sum('delta')
        .withColumnRenamed('sum(delta)','delta')
    )

    demo=DeltaTable.forName(spark,'gold.gold_demographics')
    (
        demo.alias('t')
        .merge(deltas.alias('s'),
        """t.gender=s.gender AND t.age_group=s.age_group AND t.nationality=s.nationality""")
        .whenMatchedUpdate(set={'customer_count':"t.customer_count+s.delta"})
        .whenNotMatchedInsert(values={'gender':'s.gender','age_group':'s.age_group',
        'nationality':'s.nationality',
        'customer_count':'s.delta'})
        .execute())
    (
        state.alias('t')
        .merge(new_df.alias('s'),
            't.user_id = s.user_id')
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
    print(f"Demographic batch {batch_id} processed")

demographics_query=(
    demographics_stream_df.writeStream
    .foreachBatch(update_demographics)
    .option('checkpointLocation','Files/checkpoints/demographics')
    .start()
)

geography_stream_df=(
    spark.readStream
    .format('delta')
    .table('silver.api_silver_data')
    .select(
        col('userid').alias('user_id'),
        col('Country').alias('country'),
        col('State').alias('state'),
        col('City').alias('city'),
        col("Injestion_Timestamp").alias('injection_timestamp')
    )
)

def update_geography(batch_df,batch_id):
    window_spec=Window.partitionBy('user_id').orderBy(col('injection_timestamp').desc())
    new_df=(batch_df
            .withColumn('rn',row_number().over(window_spec))
            .filter(col('rn')==1)
            .drop('rn')
            .select('user_id','country','state','city'))
    state=DeltaTable.forName(spark,'gold.gold_geography_state')
    changes=(
        new_df.alias('n')
        .join(state.toDF().alias('s'),'user_id','left')
        .select(
            col('s.country').alias("old_country"),
            col('s.state').alias('old_state'),
            col('s.city').alias('old_city'),
            col('n.country').alias("new_country"),
            col('n.state').alias('new_state'),
            col('n.city').alias('new_city')
        )
    )
    old_changes=(
        changes.filter(col('old_country').isNotNull())
        .select(
            col('old_country').alias('country'),
            col('old_state').alias('state'),
            col('old_city').alias('city'),
            lit(-1).alias('delta')
        )
    )
    new_changes=(
        changes.filter(col('new_country').isNotNull())
        .select(
            col('new_country').alias('country'),
            col('new_state').alias('state'),
            col('new_city').alias('city'),
            lit(1).alias('delta')
        )
    )
    deltas=(
        old_changes
        .unionByName(new_changes)
        .groupBy('country','state','city')
        .sum('delta')
        .withColumnRenamed('sum(delta)','delta')
    )
    geo=DeltaTable.forName(
        spark,'gold.gold_geography'
    )
    (geo.alias('t')
    .merge(deltas.alias('s'),
        "t.country=s.country AND t.state=s.state AND t.city=s.city")
        .whenMatchedUpdate(set={"customer_count":'t.customer_count+s.delta'})
        .whenNotMatchedInsert(
            values={"country":"s.country",
                    "state":"s.state",
                    "city":"s.city",
                    "customer_count":"s.delta"}
        ).execute()
    )
    (state.alias('t')
    .merge(new_df.alias('s'),"t.user_id=s.user_id")
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute())
    print(f"Geography batch {batch_id} processed")

geography_query=(
    geography_stream_df.writeStream
    .foreachBatch(update_geography)
    .option('checkpointLocation','Files/checkpoints/geography')
    .start()
)

spark.streams.awaitAnyTermination()

raise RuntimeError(
    "A Production streaming query terminated unexpectedly"
)


