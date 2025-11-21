import os
from dotenv import load_dotenv

load_dotenv()

GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY", "YOUR_API_KEY")
GOOGLE_API_KEY = os.environ.get("GOOGLE_API_KEY", "YOUR_API_KEY")
CUSTOM_SEARCH_ENGINE_ID = os.environ.get("GOOGLE_CSE_ID", "YOUR_CSE_ID")
MODEL_NAME = os.environ.get("MODEL_NAME")

TARGET_YEARS = ["2024", "2023", "2022", "2021", "2020"]
SELENIUM_LOAD_DELAY = 5
REQUEST_DELAY = 2
SEARCH_RESULTS_TO_CHECK = 5