import os
import io
import json
import sys
from . import storage

import cv2

%%% 改为通过传入的 store 使用纯函数存储；保留原语句供对照
# client = storage.storage.get_instance()

labels = ["person", "bicycle", "car", "motorcycle",
"airplane", "bus", "train", "truck", "boat", "traffic light", "fire hydrant",
"stop sign", "parking meter", "bench", "bird", "cat", "dog", "horse",
"sheep", "cow", "elephant", "bear", "zebra", "giraffe", "backpack",
"umbrella", "handbag", "tie", "suitcase", "frisbee", "skis",
"snowboard", "sports ball", "kite", "baseball bat", "baseball glove", "skateboard",
"surfboard", "tennis racket", "bottle", "wine glass", "cup", "fork", "knife",
"spoon", "bowl", "banana", "apple", "sandwich", "orange", "broccoli", "carrot", "hot dog",
"pizza", "donut", "cake", "chair", "couch", "potted plant", "bed", "dining table",
"toilet", "tv", "laptop", "mouse", "remote", "keyboard",
"cell phone", "microwave", "oven", "toaster", "sink", "refrigerator",
"book", "clock", "vase", "scissors", "teddy bear", "hair drier", "toothbrush" ]


%%% 为适配字典存储，最小变更：增加 store 参数，函数名保持不变
def load_model(store, bucket, weights_blob, config_blob, dest_dir):
    weights_path = os.path.join(dest_dir, "model.weights")
    # client.download(bucket, weights_blob, weights_path)
    storage.download(store, bucket, weights_blob, weights_path)  # %%%

    config_path = os.path.join(dest_dir, "model.config")
    # client.download(bucket, config_blob, config_path)
    storage.download(store, bucket, config_blob, config_path)    # %%%

    net = cv2.dnn.readNetFromTensorflow(weights_path, config_path)
    return net


%%% 为适配字典存储，最小变更：增加 store 参数，函数名保持不变
def load_frames(store, benchmark_bucket, bucket, blobs, dest_dir):
    for blob in blobs:
        stripped_blob = blob.replace(bucket + '/', '')
        path = os.path.join(dest_dir, stripped_blob)
        # client.download(benchmark_bucket, blob, path)
        storage.download(store, benchmark_bucket, blob, path)  # %%% blob 已含 bucket/filename
        yield cv2.imread(path)


def detect(net, img):
    rows = img.shape[0]
    cols = img.shape[1]
    img = cv2.dnn.blobFromImage(img, size=(300, 300), swapRB=True, crop=False)
    net.setInput(img)
    out = net.forward()

    preds = []
    for detection in out[0,0,:,:]:
        score = float(detection[2])
        if score > 0.5:
            class_id = int(detection[1])
            preds.append({
                "class": labels[class_id],
                "score": score
            })

    return preds


%%% 仅在需要访问“远端存储”时引入 store 参数；event 不改动
def handler(event, store):
    tmp_dir = "/tmp"

    benchmark_bucket = event["benchmark_bucket"]

    frames = list(load_frames(store, benchmark_bucket, event["frames_bucket"], event["frames"], tmp_dir))  # %%%
    net = load_model(store, benchmark_bucket, event["model_bucket"] + '/' + event["model_weights"], event["model_bucket"] + '/' + event["model_config"], tmp_dir)  # %%%

    preds = [detect(net, frame) for frame in frames]
    
    frames_names = event["frames"]
    frames_names = [x.split(".")[0] for x in event["frames"]]
    
    preds = {f"{frames_names[idx]}": dets for idx, dets in enumerate(preds)}

    %%% 为跨 Pod 传递“模拟远端存储字典”，在不改变原负载结构前提下，增加返回 store
    return store, preds

