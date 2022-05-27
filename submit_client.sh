
  
spark-submit \
    --master yarn \
    --deploy-mode client \
    --input $1\
    asm2.py \
    --output $1
