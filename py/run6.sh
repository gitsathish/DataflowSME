python -m exercises.exercise6 \
  --topic projects/YOUR_PROJECT/topics/YOUR_TOPIC \
  --output_dataset sme \
  --output_tablename exercise6 \
  --runner DataflowRunner \
  --project YOUR_PROJECT \
  --user_activity_window 240 \
  --session_gap 60 \
  --temp_location gs://YOUR_BUCKET/staging \
  --setup_file ./setup.py
