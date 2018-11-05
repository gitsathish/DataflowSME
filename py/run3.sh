python -m exercises.exercise3 \
  --topic projects/YOUR_PROJECT/topics/YOUR_TOPIC \
  --output_dataset sme \
  --output_tablename exercise3 \
  --runner DataflowRunner \
  --project YOUR_PROJECT \
  --temp_location gs://YOUR_BUCKET/staging \
  --setup_file ./setup.py
