python -m exercises.exercise0 --input gs://sme-training/game/small.csv \
  --output_dataset sme \
  --output_tablename exercise0 \
  --runner DataflowRunner \
  --project YOUR_PROJECT \
  --temp_location gs://YOUR_BUCKET/staging \
  --setup_file ./setup.py
