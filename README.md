# Social-Media-Sentiment-Performance-Analyzer

This project is our attempt to treat an artist’s social media the way a data team would treat a product: pull the data, clean it, model it, and turn it into actual decisions instead of vibes.

We focus on Gunna’s YouTube content and socials, track how different videos perform, run basic sentiment analysis on comments, and then try to connect what fans are saying to how well each video actually does over time.

---

## Short project description 

Social-Media-Sentiment-Performance-Analyzer is a small analytics project where we treat Gunna’s content like a real product. We pull video stats and comments, run sentiment analysis, and try to connect the mood of the audience to real performance metrics like views, likes, and engagement. The point is to move past “this feels like it did well” and start backing everything up with data.

---

## Business / Analytics Goal

- Understand which Gunna videos have the most positive vs. negative sentiment over time  
- See whether more positive sentiment is linked to higher likes, views, and engagement  
- Pull real comment examples to show what fans are actually saying, not just what the numbers look like  
- Highlight which types of videos (song drops, visuals, collabs, etc.) create the best combo of sentiment + performance  
- Build a simple, repeatable framework that could be reused for other artists or creators

---

## Project Summary

We collected data on a set of Gunna’s recent official YouTube videos and pulled both the performance stats (views, likes, comments) and the raw comment text. In total, the dataset covers millions of views, tens of thousands of comments, and multiple different types of videos (solo tracks, features, different visual styles).

Using Python, we cleaned the comments, ran sentiment analysis, and labeled each comment as positive, neutral, or negative with an associated sentiment score. We then aggregated that up to the video level so each song has an average sentiment score and a percentage of comments that are positive, alongside its views, likes, and total comments.

We built a Power BI report with three main pages: an overview page with KPI cards (total views, total comments, average sentiment, and % positive), a bar chart of average sentiment by video, and a time view; a “Sentiment vs Engagement” page with a scatterplot that compares average sentiment to likes (bubble size = comments); and a comment drilldown page where we can filter down to one video and read the actual positive or negative comments behind the numbers.

At a high level, the mood in the comments is mostly positive but not overwhelmingly so. A few songs clearly stand out with much higher average sentiment and a higher share of positive comments, and those usually line up with strong engagement as well. There are also tracks that pull big numbers on views and likes but have more mixed comment sections, which shows that high engagement doesn’t always mean everyone loves the direction of the song.

From an artist or label perspective, this setup gives a quick way to see which songs generate the best combination of hype and genuine positive reaction, and which ones are more polarizing. If we were on Gunna’s team, we’d use this type of dashboard as a release “health check”: track sentiment in the first few days after a drop, pay attention to what fans repeat in the top comments, and double down on the sounds and visuals that consistently show up with high sentiment and strong performance.

---

## What this repo includes

- Python scripts for:
  - Collecting and cleaning social media / streaming data  
  - Basic sentiment analysis on comments  
  - Feature engineering (post type, collab flag, release “era,” etc.)
- SQL queries to structure everything in a relational database
- Example analysis notebooks (EDA, correlations, simple prediction)
- A reporting layer (Power BI / notebooks) that turns the data into:
  - Dashboards
  - Simple, plain-English insights for artists/creators

---

## Why build this

We’re a CS + marketing/data combo who love music and creator analytics.  
This project is part portfolio piece, part sandbox to practice “real” analytics work:

- Taking messy behavior data  
- Turning it into clean tables  
- Asking good questions  
- Backing answers with numbers instead of guesses  

If you’ve got feedback, ideas, or want to extend this for another artist/creator, feel free to open an issue or fork it.
