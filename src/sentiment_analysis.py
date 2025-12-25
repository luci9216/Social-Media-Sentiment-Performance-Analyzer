import pandas as pd
from textblob import TextBlob
import os

RAW_COMMENTS_PATH = "data/raw/comments_raw.csv"
PROCESSED_COMMENTS_PATH = "data/processed/comments_with_sentiment.csv"

def get_sentiment_score(text: str) -> float:
    if not isinstance(text, str) or text.strip() == "":
        return 0.0
    blob = TextBlob(text)
    return blob.sentiment.polarity

def label_sentiment(score: float) -> str:
    if score > 0.1:
        return "Positive"
    elif score < -0.1:
        return "Negative"
    else:
        return "Neutral"

def main():
    os.makedirs("data/processed", exist_ok=True)

    print(f"Loading {RAW_COMMENTS_PATH}...")
    df = pd.read_csv(RAW_COMMENTS_PATH)

    print("Calculating sentiment scores...")
    df["sentiment_score"] = df["comment_text"].apply(get_sentiment_score)
    df["sentiment_label"] = df["sentiment_score"].apply(label_sentiment)

    df.to_csv(PROCESSED_COMMENTS_PATH, index=False)
    print(f"Saved {PROCESSED_COMMENTS_PATH}")

if __name__ == "__main__":
    main()
