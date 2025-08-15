from transformers import MarianMTModel, MarianTokenizer
import os

MODEL_NAME = "Helsinki-NLP/opus-mt-en-fr"
MODEL_DIR = os.path.abspath(os.path.dirname(__file__))
MODEL_PATH = os.path.join(MODEL_DIR, "opus-mt-en-fr")

def download_model():
    print(f"Downloading model to {MODEL_PATH} ...")
    MarianTokenizer.from_pretrained(MODEL_NAME, cache_dir=MODEL_PATH)
    MarianMTModel.from_pretrained(MODEL_NAME, cache_dir=MODEL_PATH)
    print("Model downloaded.")

if __name__ == "__main__":
    download_model()
