# project-big-data-2023
# Scenario 1 Bar Graph
  
</div>

<p align="justify">
  Dalam langkah ini, kita membaca data dari file CSV.Kemudian, kita melakukan pembersihan data dengan menghapus baris duplikat dan baris dengan nilai null.
Lalu membuat kolom baru dengan nama "tahun" yang memiliki tipe data Integer. Selanjutnya mengelompokkan data berdasarkan kolom "kode_provinsi" dan "tahun". Kemudian, kita menggunakan fungsi agg() dan avg() untuk menghitung rata-rata dari kolom "jumlah_penduduk". Alias "avg_jumlah_penduduk" digunakan untuk memberi nama kolom hasil perhitungan. Lalu kita menggunakan metode join() untuk bergabung antara DataFrame awal (df) dengan DataFrame hasil perhitungan rata-rata (avg_jumlah_penduduk_df). Kedua DataFrame digabungkan berdasarkan kolom "kode_provinsi" dan "tahun".
</p>
<div>
  <pre>
    <code>
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg
from pyspark.sql.types import IntegerType

# Inisialisasi SparkSession
spark = SparkSession.builder.getOrCreate()

# Membaca data dari file CSV
df = spark.read.csv("/content/drive/MyDrive/Big-Data/big-data.csv", header=True, inferSchema=True)
df = df.dropDuplicates().na.drop()

# Mengubah tipe data kolom tahun menjadi Integer
df = df.withColumn("tahun", df["tahun"].cast(IntegerType()))

# Menghitung rata-rata jumlah_penduduk berdasarkan kode_provinsi dan tahun
avg_jumlah_penduduk_df = df.groupBy("kode_provinsi", "tahun").agg(avg("jumlah_penduduk").alias("avg_jumlah_penduduk"))

# Bergabung dengan DataFrame awal untuk mendapatkan kolom avg_jumlah_penduduk
df_with_avg_jumlah_penduduk = df.join(avg_jumlah_penduduk_df, ["kode_provinsi", "tahun"])

# Menampilkan hasil
df_with_avg_jumlah_penduduk.show()
    </code>
  </pre>
</div>

## Machine Learning Scenarios Flowchart
<img src="PPT/Frame 24.png" />

## Machine Learning Scenarios Implementation
<img src="docs/skenario_1.png" />

## Result Machine Learning Scenarios
<img src="docs/hasil_skenario_1.png" />

## Visualization Machine Learning Scenarios
<img src="docs/atas ke bawah.png" />
<img src="docs/bawah ke atas.png" />

<div align="center">

# Scenario 2 Bar Graph
  
</div>

<p align="justify">
Dalam skenario ini, kode di bawah merupakan implementasi Spark Machine Learning dalam konteks analisis dataset "covid.csv" untuk membandingkan kebijakan pemerintah yang diambil di berbagai negara dalam menangani pandemi Covid-19 dan dampaknya terhadap angka kasus dan kematian. Prosesnya dimulai dengan inisialisasi sesi Spark menggunakan SparkSession, pembacaan dataset ke dalam DataFrame, dan pra-pemrosesan data untuk menghapus duplikat dan nilai yang hilang. Selanjutnya, dilakukan pengolahan data untuk menghitung rata-rata jumlah kasus, jumlah kematian, dan indeks ketaatan kebijakan (stringency index) untuk setiap lokasi dan tahun menggunakan fungsi agregasi pada DataFrame. Hasilnya digabungkan kembali dengan DataFrame asli untuk memperoleh informasi rata-rata yang sesuai dengan lokasi dan tahunnya. Fitur-fitur yang relevan diubah menjadi vektor menggunakan VectorAssembler.
<br><br>
Data yang telah diolah kemudian dibagi menjadi training set dan testing set dengan perbandingan 80:20. Model regresi linear menggunakan LinearRegression dari PySpark diinisialisasi, dilatih menggunakan training set, dan digunakan untuk memprediksi nilai rata-rata indeks ketaatan kebijakan pada testing set. Kinerja model dievaluasi menggunakan metrik RMSE (Root Mean Square Error) dengan membandingkan nilai prediksi dengan nilai sebenarnya pada testing set.
<br><br>
Selain itu, hasil analisis divisualisasikan dalam bentuk grafik batang menggunakan library Matplotlib. Grafik tersebut menampilkan 10 negara unik dengan rata-rata indeks ketaatan kebijakan tertinggi, rata-rata jumlah kasus tertinggi, dan rata-rata jumlah kematian tertinggi. Hal ini memberikan informasi visual mengenai negara-negara dengan tingkat ketaatan kebijakan, jumlah kasus, dan jumlah kematian yang lebih tinggi. Keseluruhan kode ini menjelaskan implementasi Spark Machine Learning dalam analisis kebijakan pemerintah selama pandemi Covid-19 dengan melibatkan proses data, pelatihan model regresi linear, evaluasi model, dan visualisasi hasil analisis.
</p>
<div>
  <pre>
    <code>
from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql.functions import avg, year
from pyspark.sql.functions import desc
import matplotlib.pyplot as plt
spark = SparkSession.builder.appName("GovernmentPolicyAnalysis").getOrCreate()
df = spark.read.csv("/covid.csv", header=True, inferSchema=True)
df = df.dropDuplicates().na.drop()
df = df.withColumn("year", year(df.date))
avg_cases_deaths_df = df.groupBy("location", "year").agg(avg("total_cases").alias("avg_total_cases"), avg("total_deaths").alias("avg_total_deaths"), avg("stringency_index").alias("avg_stringency_index"))
df_with_avg_cases_deaths = df.join(avg_cases_deaths_df, ["location", "year"])
selected_columns = ["location", "year", "avg_total_cases", "avg_total_deaths", "avg_stringency_index"]
assembler = VectorAssembler(inputCols=selected_columns[2:], outputCol="features")
df_transformed = assembler.transform(df_with_avg_cases_deaths)
(trainingData, testData) = df_transformed.randomSplit([0.8, 0.2])
lr = LinearRegression(featuresCol="features", labelCol="avg_stringency_index")
model = lr.fit(trainingData)
predictions = model.transform(testData)
evaluator = RegressionEvaluator(labelCol="avg_stringency_index", metricName="rmse")
rmse = evaluator.evaluate(predictions)
print("Root Mean Square Error (RMSE):", rmse)
top_10_countries = df_with_avg_cases_deaths.select("location", "avg_stringency_index", "avg_total_cases", "avg_total_deaths") \
    .dropDuplicates(["location"]) \
    .orderBy("avg_total_cases", ascending=False) \
    .limit(10)
top_10_countries_pd = top_10_countries.toPandas()
fig, ax = plt.subplots(figsize=(12, 6))
ax.bar(top_10_countries_pd["location"], top_10_countries_pd["avg_stringency_index"], label="Average Stringency Index")
ax.bar(top_10_countries_pd["location"], top_10_countries_pd["avg_total_cases"], label="Average Total Cases")
ax.bar(top_10_countries_pd["location"], top_10_countries_pd["avg_total_deaths"], label="Average Total Deaths")
ax.set_xlabel("Country")
ax.set_ylabel("Average Values")
ax.set_title("Top 10 Unique Countries with Highest Averages")
ax.legend()
plt.xticks(rotation=45)
plt.show()
    </code>
  </pre>
</div>

## Machine Learning Scenarios Flowchart
<img src="PPT/Frame 30.png" />

## Machine Learning Scenarios Implementation
<img src="docs/skenario_2.png" />

## Result Machine Learning Scenarios
<img src="docs/hasil_skenario_2.png" />

## Visualization Machine Learning Scenarios
<img src="docs/tinggi.png" />
<img src="docs/rendah.png" />

<div align="center">
