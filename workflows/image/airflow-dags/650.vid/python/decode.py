import os
import uuid
from . import storage

import cv2

%%% 改为通过传入的 store 使用纯函数存储，避免全局客户端副作用；保留原语句供对照
# client = storage.storage.get_instance()


def chunks(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


%%% 为适配字典存储，最小变更：增加 store 参数，函数名保持不变
def load_video(store, benchmark_bucket, bucket, blob, dest_dir):
    path = os.path.join(dest_dir, blob)
    # client.download(benchmark_bucket, bucket + '/' + blob, path)
    storage.download(store, benchmark_bucket, bucket + '/' + blob, path)  # %%% 使用字典存储读取对象
    return path


def decode_video(path, n_frames, dest_dir):
    vidcap = cv2.VideoCapture(path)
    success, img = vidcap.read()
    img_paths = []
    while success and len(img_paths) < n_frames:
        img_path = os.path.join(dest_dir, f"frame{len(img_paths)}.jpg")
        img_paths.append(img_path)
        cv2.imwrite(img_path, img)
        success, img = vidcap.read()

    return img_paths


%%% 为适配字典存储，最小变更：增加 store 参数，函数名保持不变；返回 (new_store, keys)
def upload_imgs(benchmark_bucket, bucket, paths, store):
    # client = storage.storage.get_instance()
    keys = []
    new_store = store
    for path in paths:
        name = os.path.basename(path)
        # yield client.upload(benchmark_bucket, bucket + '/' + name, path)
        new_store, _ = storage.upload(new_store, benchmark_bucket, bucket + '/' + name, path)  # %%% 写入字典存储
        keys.append(bucket + '/' + name)  # %%% analyse 期望的是 "frames_bucket/文件名"
    return new_store, keys


%%% 仅在需要访问“远端存储”时引入 store 参数；event 不改动
def handler(event, store):
    vid_blob = event["video"]
    n_frames = event["n_frames"]
    batch_size = event["batch_size"]
    frames_bucket = event["frames_bucket"]
    input_bucket = event["input_bucket"]
    benchmark_bucket = event["benchmark_bucket"]

    tmp_dir = os.path.join("/tmp", str(uuid.uuid4()))
    os.makedirs(tmp_dir, exist_ok=True)

    vid_path = load_video(store, benchmark_bucket, input_bucket, vid_blob, tmp_dir)  # %%% 通过字典存储下载视频
    img_paths = decode_video(vid_path, n_frames, tmp_dir)
    new_store, keys = upload_imgs(benchmark_bucket, frames_bucket, img_paths, store)  # %%% 通过字典存储上传帧
    frames = list(chunks(keys, batch_size))

    %%% 为跨 Pod 传递“模拟远端存储字典”，在不改变原负载结构前提下，增加返回 new_store
    return new_store, {
        "frames": [{
            "frames_bucket": frames_bucket,
            "frames": fs,
            "benchmark_bucket": benchmark_bucket,
            "model_bucket": input_bucket,
            "model_config": event["model_config"],
            "model_weights": event["model_weights"]
        } for fs in frames]
    }
