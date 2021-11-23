import collections
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType
import pyspark.sql.functions as f

BmiCategory_HealthRisk = collections.namedtuple('BmiCategory_HealthRisk', ['category', 'risk'])


def square(height):
    return height * height


def to_meter(height):
    """
    :param height: in centimeter
    :return: height in meter
    """
    return height / 100


def bmi(mass, height):
    """
    :param mass: the person's mass in kg
    :param height: the person's height in cm
    :return: the person's bmi
    """
    return mass / square(to_meter(height))


def get_category_risk(bmi_value):
    category_risk = None
    if bmi_value <= 18.4:
        category_risk = BmiCategory_HealthRisk("Underweight", "Malnutrition risk")
    elif 18.5 <= bmi_value <= 24.9:
        category_risk = BmiCategory_HealthRisk("Normal weight", "Low risk")
    elif 25 <= bmi_value <= 29.9:
        category_risk = BmiCategory_HealthRisk("Overweight", "Enhanced risk")
    elif 30 <= bmi_value <= 34.9:
        category_risk = BmiCategory_HealthRisk("Moderately obese", "Medium risk")
    elif 35 <= bmi_value <= 39.9:
        category_risk = BmiCategory_HealthRisk("Severely obese", "High risk")
    elif bmi_value >= 40:
        category_risk = BmiCategory_HealthRisk("Very severely obese", "Very high risk")
    return category_risk


def get_bmi_category(bmi_value):
    return get_category_risk(bmi_value).category


def get_health_risk(bmi_value):
    return get_category_risk(bmi_value).risk


bmi_category_udf = udf(lambda x: get_bmi_category(x), StringType())
health_risk_udf = udf(lambda x: get_health_risk(x), StringType())


def overweight_people_filter():
    return """ "BMI Category" === Overweight """


if __name__ == '__main__':
    """"""
    os.environ['PYSPARK_PYTHON'] = '/usr/bin/python3'
    os.environ['PYSPARK_DRIVER_PYTHON'] = '/usr/bin/python3' \
                                          """"""
    spark_session = SparkSession.builder \
        .master("local[*]") \
        .appName("bmi_calculation").getOrCreate()

    df = spark_session.read \
        .option("mode", "permissive") \
        .json(f"..{os.path.sep}resources{os.path.sep}data.json") \
        .withColumn("BMI", bmi(col("WeightKg"), col("HeightCm"))) \
        .withColumn("BMI Category", bmi_category_udf(col("BMI"))) \
        .withColumn("Health risk", health_risk_udf(col("BMI")))
    df.show()

    overweight_people = df.filter(f.col("BMI Category") == "Overweight").count()
    print("People with overweight size", overweight_people)
    """"""
