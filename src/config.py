import yaml
import os

def load_config(path: str) -> dict:
    with open(path) as f:
        cfg = yaml.safe_load(f)
    if os.environ.get("KALSHI_API_KEY_ID"):
        cfg["kalshi"]["api_key_id"] = os.environ["KALSHI_API_KEY_ID"]
    if os.environ.get("KALSHI_PRIVATE_KEY_PATH"):
        cfg["kalshi"]["private_key_path"] = os.environ["KALSHI_PRIVATE_KEY_PATH"]
    if os.environ.get("SMS_TO"):
        cfg["alerts"]["sms_to"] = os.environ["SMS_TO"]
    if os.environ.get("SMS_FROM"):
        cfg["alerts"]["sms_from"] = os.environ["SMS_FROM"]
    return cfg
