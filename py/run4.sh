python -m exercises.exercise4 \
  --topic projects/YOUR_PROJECT/topics/YOUR_TOPIC \
  --output_dataset sme \
  --output_tablename exercise4 \
  --runner DataflowRunner \
  --project YOUR_PROJECT \
  --temp_location gs://YOUR_BUCKET/staging \
  --setup_file ./setup.py
