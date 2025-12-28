import pandas as pd
import os

VIDEO_STATS_RAW_PATH = "data/raw/video_stats_raw.csv"
COMMENTS_SENTIMENT_PATH = "data/processed/comments_with_sentiment.csv"
VIDEO_SUMMARY_PATH = "data/processed/video_sentiment_summary.csv"

def main():
    os.makedirs("data/processed", exist_ok=True)

    print("Loading raw video stats and comments with sentiment...")
    video_stats_df = pd.read_csv(VIDEO_STATS_RAW_PATH)
    comments_df = pd.read_csv(COMMENTS_SENTIMENT_PATH)

    print("Aggregating sentiment by video...")
    agg = comments_df.groupby("video_id").agg(
        avg_sentiment=("sentiment_score", "mean"),
        pct_positive=("sentiment_label", lambda x: (x == "Positive").mean()),
        pct_negative=("sentiment_label", lambda x: (x == "Negative").mean()),
        comment_count=("comment_text", "count")
    ).reset_index()

    print("Merging with video stats...")
    merged = video_stats_df.merge(agg, on="video_id", how="left")

    merged.to_csv(VIDEO_SUMMARY_PATH, index=False)
    print(f"Saved {VIDEO_SUMMARY_PATH}")

if __name__ == "__main__":
    main()
