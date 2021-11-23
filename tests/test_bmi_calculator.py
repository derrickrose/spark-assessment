from spark_assessment import __version__
from spark_assessment.bmi_calculator import get_bmi_category, get_health_risk

DEFAULT_BI = 26


def test_version():
    assert __version__ == '0.1.0'


def getBmiCategory_shouldReturn_Overweight():
    assert get_bmi_category(DEFAULT_BI) == "Overweight"


def getHealthRisk_shouldReturn_EnhancedRisk():
    assert get_health_risk(DEFAULT_BI) == "Enhanced risk"
