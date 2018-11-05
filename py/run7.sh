python -m exercises.exercise7 \
  --topic projects/YOUR_PROJECT/topics/YOUR_TOPIC \
  --play_topic projects/YOUR_PROJECT/topics/YOUR_TOPIC-play \
  --output_dataset sme \
  --output_tablename exercise7 \
  --runner DataflowRunner \
  --project YOUR_PROJECT \
  --session_gap 20 \
  --temp_location gs://YOUR_BUCKET/staging \
  --setup_file ./setup.py
