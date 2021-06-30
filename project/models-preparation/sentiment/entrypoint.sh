#!/bin/bash

python /opt/conda/lib/python3.8/site-packages/transformers/convert_graph_to_onnx.py \
    --pipeline sentiment-analysis \
    --opset 12 \
    --framework pt \
    --model cardiffnlp/twitter-xlm-roberta-base-sentiment \
    --quantize \
    /onnx/sentiment/1/model.onnx
rm /onnx/sentiment/1/model.onnx
rm /onnx/sentiment/1/model-optimized-quantized.onnx
mv /onnx/sentiment/1/model-optimized.onnx /onnx/sentiment/1/model.onnx
