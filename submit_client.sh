
  
spark-submit \
    --master yarn \
    --deploy-mode client \
    asm2.py \
    --output $1
