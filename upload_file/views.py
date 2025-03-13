import os
from django.shortcuts import render
import matplotlib.pyplot as plt
import seaborn as sns
from django.conf import settings
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName('csv_analyzer').getOrCreate()


def home(request):
    
    return render(request,'home.html',{})

def upload(request):
    if request.method == 'POST':
        upload_file = request.FILES['file'] #uploaded file

        # upload_file_name = upload_file.name.split(".")[0]

        file_path = os.path.join(settings.MEDIA_ROOT,upload_file.name)

        os.makedirs(settings.MEDIA_ROOT,exist_ok = True)
    
        with open(file_path, 'wb+') as f:
            for chunk in upload_file.chunks():
                f.write(chunk)

        filters = {key:value for key in request.POST.items() if key in ['Airline','Source','Destination']}
        
        result = process_data(file_path,filters)

        return render(request,'result.html',result)

    return render(request,'upload_file.html')

def plot_airline_counts(df):
    viz_dir = os.path.join(settings.MEDIA_ROOT, "visualizations")  # ✅ Correct path
    os.makedirs(viz_dir, exist_ok=True)  # ✅ Ensure the directory exists

    airline_counts = (
        df.groupBy("Airline").count().toPandas()  
        if "Airline" in df.columns else None
    )

    if airline_counts is not None:
        plt.figure(figsize=(8, 4))
        sns.barplot(y=airline_counts["Airline"], x=airline_counts["count"], palette="viridis")
        plt.title("Number of Flights by Airline")
        plt.xlabel("Count")
        plt.ylabel("Airline")
        plt.tight_layout()
        airline_plot_path = os.path.join(viz_dir, "airline_counts.png")
        plt.savefig(airline_plot_path)
        plt.close()
        return airline_plot_path
    return None

# def plot_airline_counts(df):
#     airline_counts = (
#         df.groupBy("Airline").count().toPandas()  # Small data, okay to convert
#         if "Airline" in df.columns else None
#     )

#     if airline_counts is not None:
#         plt.figure(figsize=(8, 4))
#         sns.barplot(y=airline_counts["Airline"], x=airline_counts["count"], palette="viridis")
#         plt.title("Number of Flights by Airline")
#         plt.xlabel("Count")
#         plt.ylabel("Airline")
#         plt.tight_layout()
#         airline_plot_path = os.path.join("media/visualizations", "airline_counts.png")
#         plt.savefig(airline_plot_path)
#         plt.close()
#         return airline_plot_path
#     return None

def plot_price_distribution(df):
    viz_dir = os.path.join(settings.MEDIA_ROOT, "visualizations")  # ✅ Correct path
    os.makedirs(viz_dir, exist_ok=True)  # ✅ Ensure the directory exists

    if "Price" in df.columns:
        prices = [row["Price"] for row in df.select("Price").collect()]  
        plt.figure(figsize=(8, 4))
        sns.histplot(prices, bins=30, kde=True, color="blue")
        plt.title("Price Distribution of Flights")
        plt.xlabel("Price")
        plt.ylabel("Frequency")
        plt.tight_layout()
        price_plot_path = os.path.join(viz_dir, "price_distribution.png")
        plt.savefig(price_plot_path)
        plt.close()
        return price_plot_path
    return None

# def plot_price_distribution(df):
#     if "Price" in df.columns:
#         prices = [row["Price"] for row in df.select("Price").collect()]  # Collecting only necessary data
#         plt.figure(figsize=(8, 4))
#         sns.histplot(prices, bins=30, kde=True, color="blue")
#         plt.title("Price Distribution of Flights")
#         plt.xlabel("Price")
#         plt.ylabel("Frequency")
#         plt.tight_layout()
#         price_plot_path = os.path.join("media/visualizations", "price_distribution.png")
#         plt.savefig(price_plot_path)
#         plt.close()
#         return price_plot_path
#     return None
# 
'''# Clean_df.groupBy("Airline").count().write.csv("media/airline_counts.csv", header=True)
# Clean_df.select("Price").write.csv("media/price_distribution.csv", header=True)'''

def process_data(file_path,filters):
    
    df = spark.read.format('csv').option('header',True).option('inferSchema',True).load(file_path)

    Clean_df = df.dropna()

    if 'Price' in Clean_df.columns:
        Clean_df = Clean_df.withColumn('Price',Clean_df['Price'].cast('double'))
    
    for key,value in filters.items():
        if key in Clean_df.columns:
            Clean_df = Clean_df.filter(Clean_df[key]==value)
    
    # pdf = Clean_df.toPandas()
    viz_dir = 'media/vizualize'

    os.makedirs(viz_dir,exist_ok=True)

    airline_plot_path = plot_airline_counts(Clean_df)
    price_plot_path = plot_price_distribution(Clean_df)

    avg_price = Clean_df.select(avg("Price")).collect()[0][0] if "Price" in Clean_df.columns else None

    flight_counts = {row["Airline"]: row["count"] for row in Clean_df.groupBy("Airline").count().collect()} if "Airline" in Clean_df.columns else {}



    # if "Airline" in pdf.columns:
    #     plt.figure(figsize=(8, 4))
    #     sns.countplot(y=pdf["Airline"], order=pdf["Airline"].value_counts().index, palette="viridis")
    #     plt.title("Number of Flights by Airline")
    #     plt.xlabel("Count")
    #     plt.ylabel("Airline")
    #     plt.tight_layout()
    #     airline_plot_path = os.path.join(viz_dir, "airline_counts.png")
    #     plt.savefig(airline_plot_path)
    #     plt.close()
    # else:
    #     airline_plot_path = None


    # if "Price" in pdf.columns:
    #     plt.figure(figsize=(8, 4))
    #     sns.histplot(pdf["Price"], bins=30, kde=True, color="blue")
    #     plt.title("Price Distribution of Flights")
    #     plt.xlabel("Price")
    #     plt.ylabel("Frequency")
    #     plt.tight_layout()
    #     price_plot_path = os.path.join(viz_dir, "price_distribution.png")
    #     plt.savefig(price_plot_path)
    #     plt.close()
    # else:
    #     price_plot_path = None

    # avg_price = pdf["Price"].mean() if "Price" in pdf.columns else None
    # flight_counts = pdf["Airline"].value_counts().to_dict() if "Airline" in pdf.columns else {}

    return {
        "columns": list(Clean_df.columns),
        "avg_price": avg_price,
        "flight_counts": flight_counts,
        "airline_plot": airline_plot_path,
        "price_plot": price_plot_path
    }