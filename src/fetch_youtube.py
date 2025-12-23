import os
import pandas as pd
from googleapiclient.discovery import build
from config import YOUTUBE_API_KEY

GUNNA_CHANNEL_ID = "UCAkIMkEaa9sZmjcy7mfd5lQ"

def get_youtube_client():
    return build("youtube", "v3", developerKey=YOUTUBE_API_KEY)

def get_latest_official_videos(youtube, channel_id, target_count=11):
    ch_request = youtube.channels().list(part="contentDetails", id=channel_id)
    ch_response = ch_request.execute()
    uploads_id = ch_response["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]

    official_videos = []
    next_page_token = None

    while len(official_videos) < target_count:
        v_request = youtube.playlistItems().list(
            part="snippet,contentDetails",
            playlistId=uploads_id,
            maxResults=50,
            pageToken=next_page_token
        )
        v_response = v_request.execute()

        for item in v_response["items"]:
            title = item["snippet"]["title"]
            # Filter, only keep if ends in [Official Video]
            if title.strip().endswith("[Official Video]"):
                official_videos.append({
                    "video_id": item["contentDetails"]["videoId"],
                    "title": title,
                    "publish_time": item["snippet"]["publishedAt"]
                })

                if len(official_videos) == target_count: 
                    break

        next_page_token = v_response.get("nextPageToken")
        # Stop when theres no more pages or target is found
        if not next_page_token or len(official_videos) >= target_count: 
            break

    return pd.DataFrame(official_videos)

def get_video_stats(youtube, video_ids):
    stats_request = youtube.videos().list(
        part="statistics",
        id=",".join(video_ids)
    )
    stats_response = stats_request.execute()

    stats = []
    for item in stats_response["items"]:
        stats.append({
            "video_id": item["id"],
            "view_count": int(item["statistics"].get("viewCount",0)),
            "like_count": int(item["statistics"].get("likeCount",0)),
            "comment_count": int(item["statistics"].get("commentCount", 0))
        })
    return pd.DataFrame(stats)
    
def get_comments(youtube, video_id, max_comments=100):
    comments = []
    try:
        request = youtube.commentThreads().list(
            part="snippet",
            videoId=video_id,
            maxResults=max_comments,
            textFormat="plainText"
        )
        response = request.execute()

        for item in response["items"]:
            snippet = item["snippet"]["topLevelComment"]["snippet"]
            comments.append({
                "video_id": video_id,
                "comment_text": snippet["textDisplay"],
                "comment_likes": snippet["likeCount"],
                "comment_date": snippet["publishedAt"]
            })
    except Exception as e:
        print(f"Skipping comments for {video_id} (likely disabled).")
    return comments

def main():
    youtube = get_youtube_client()
    
    # Step 1: Find the videos
    videos_df = get_latest_official_videos(youtube, GUNNA_CHANNEL_ID)
    
    if len(videos_df) < 1:
        return

    for i, row in videos_df.iterrows():
        print(f"   - {row['title']}")

    # Step 2: Get Stats (Views/Likes)
    stats_df = get_video_stats(youtube, videos_df["video_id"].tolist())
    final_stats = videos_df.merge(stats_df, on="video_id")

    os.makedirs("data/raw", exist_ok=True)
    final_stats.to_csv("data/raw/video_stats_raw.csv", index=False)

    # Step 3: Get Comments
    all_comments = []
    for v_id in videos_df["video_id"]:
        all_comments.extend(get_comments(youtube, v_id))
    
    pd.DataFrame(all_comments).to_csv("data/raw/comments_raw.csv", index=False)

if __name__ == "__main__":
    main()
