import pendulum
from airflow.decorators import dag, task
import logging
from functools import wraps
from time import time
import os
import io
import uuid
import cv2


# by Jonathan Prieto-Cubides https://stackoverflow.com/questions/1622943/timeit-versus-timing-decorator
def timing(f):
    @wraps(f)
    def wrap(*args, **kw):
        ts = time()
        result = f(*args, **kw)
        te = time()
        logging.info('func:%r args:[%r, %r] took: %f sec. Start: %f, End: %f' % (f.__name__, args, kw, te-ts, ts, te))
        return result
    return wrap

#%%% ======================== 存储（字典模拟远端对象存储）========================
#%%% 说明：所有“远端存储”操作都改为读写此字典；在任务间通过 XCom 显式传递
def init_store() -> dict:
    return {}

def _ensure_bucket(store: dict, bucket: str) -> dict:
    if bucket not in store:
        store[bucket] = {}
    return store

def put_bytes(store: dict, bucket: str, key: str, data: bytes) -> tuple[dict, str]:
    _ensure_bucket(store, bucket)
    store[bucket][key] = bytes(data)
    return store, f"{bucket}/{key}"

def upload(store: dict, bucket: str, key: str, local_path: str) -> tuple[dict, str]:
    with open(local_path, "rb") as f:
        data = f.read()
    return put_bytes(store, bucket, key, data)

def upload_stream(store: dict, bucket: str, key: str, stream: io.BytesIO) -> tuple[dict, str]:
    stream.seek(0)
    data = stream.read()
    return put_bytes(store, bucket, key, data)

def get_bytes(store: dict, bucket: str, key: str) -> bytes:
    return store[bucket][key]

def download(store: dict, bucket: str, key: str, dest_path: str) -> None:
    data = get_bytes(store, bucket, key)
    os.makedirs(os.path.dirname(dest_path), exist_ok=True)
    with open(dest_path, "wb") as f:
        f.write(data)

def download_stream(store: dict, bucket: str, key: str) -> io.BytesIO:
    return io.BytesIO(get_bytes(store, bucket, key))

def list_directory(store: dict, bucket: str, prefix: str) -> list[str]:
    if bucket not in store:
        return []
    return [key for key in store[bucket].keys() if key.startswith(prefix)]

#%%% ======================== 业务内联：input / decode / analyse / summarize ========================
#%%% 为尽量贴近源代码，函数名/变量名保持一致，仅在返回值附加 store（或 new_store）

def chunks(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

def load_video(store, benchmark_bucket, bucket, blob, dest_dir):
    path = os.path.join(dest_dir, blob)
    download(store, benchmark_bucket, bucket + '/' + blob, path)  # %%% 从“字典存储”下载
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

def upload_imgs(benchmark_bucket, bucket, paths, store):
    keys = []
    for path in paths:
        name = os.path.basename(path)
        store, _ = upload(store, benchmark_bucket, bucket + '/' + name, path)  # %%% 写入“字典存储”
        keys.append(bucket + '/' + name)
    return store, keys

def input_generate(data_dir, size, benchmarks_bucket, input_buckets, output_buckets):
   #%%% 内联自 650.vid/input.py 的 generate_input（去掉 upload_func 参数，改为直接写 store 由调用方提供）
    size_generators = {
        "test": (3, 10, "video_test.mp4"),
        "small": (10, 5, "video_small.mp4"),
        "large": (1000, 3, "video_large.mp4"),
    }
    n_frames, batch_size, video_name = size_generators[size]
    files = ["frozen_inference_graph.pb", "faster_rcnn_resnet50_coco_2018_01_28.pbtxt", video_name]
    return {
        "video": video_name,
        "n_frames": n_frames,
        "batch_size": batch_size,
        "frames_bucket": output_buckets[0],
        "benchmark_bucket": benchmarks_bucket,
        "input_bucket": input_buckets[0],
        "model_weights": files[0],
        "model_config": files[1],
        "_files": files,  # %%% 仅供上游上传时使用，不传下游
    }

def load_model(store, bucket, weights_blob, config_blob, dest_dir):
    weights_path = os.path.join(dest_dir, "model.weights")
    download(store, bucket, weights_blob, weights_path)
    config_path = os.path.join(dest_dir, "model.config")
    download(store, bucket, config_blob, config_path)
    net = cv2.dnn.readNetFromTensorflow(weights_path, config_path)
    return net

def load_frames(store, benchmark_bucket, bucket, blobs, dest_dir):
    for blob in blobs:
        stripped_blob = blob.replace(bucket + '/', '')
        path = os.path.join(dest_dir, stripped_blob)
        download(store, benchmark_bucket, blob, path)
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
            preds.append({"class": str(class_id), "score": score})  # %%% 简化：不内联完整 labels
    return preds

def summarize_handler(event):
    frames = event["frames"]
    logs = {}
    for xs in frames:
        for key, value in xs.items():
            logs[key] = value
    return logs

@dag(
    schedule_interval=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    is_paused_upon_creation=False)
def benchmark_w2_d3():
    # %%% 原示例任务保留为注释
    # @task
    # @timing
    # def extract():
    #     # dummy data source
    #     return [10] * 2

    # @task
    # @timing
    # def stage_00(x: int):
    #     return x + x

    # @task
    # @timing
    # def do_sum(values):
    #     return sum(values)

    #%%% 第一层：input（size=small）+ decode（写帧到“字典存储”）
    @task
    @timing
    def input_and_decode():
        store = init_store()
        data_dir = os.getenv("VID_DATA_DIR", "/data")  # %%% 可通过环境变量配置数据目录
        benchmarks_bucket = "bench"
        input_bucket = "inputs"
        frames_bucket = "frames"

        evt = input_generate(data_dir, "small", benchmarks_bucket, [input_bucket], [frames_bucket])

        # %%% 上传模型与视频到“字典存储”；若文件不存在会抛错，请确保容器内存在这些文件
        for name in evt["_files"]:
            local_path = os.path.join(data_dir, name)
            store, _ = upload(store, benchmarks_bucket, input_bucket + '/' + name, local_path)

        tmp_dir = os.path.join("/tmp", str(uuid.uuid4()))
        os.makedirs(tmp_dir, exist_ok=True)
        vid_path = load_video(store, benchmarks_bucket, input_bucket, evt["video"], tmp_dir)
        img_paths = decode_video(vid_path, evt["n_frames"], tmp_dir)
        store, keys = upload_imgs(benchmarks_bucket, frames_bucket, img_paths, store)
        frames = list(chunks(keys, evt["batch_size"]))
        frames_payload = {
            "frames": [{
                "frames_bucket": frames_bucket,
                "frames": fs,
                "benchmark_bucket": benchmarks_bucket,
                "model_bucket": input_bucket,
                "model_config": evt["model_config"],
                "model_weights": evt["model_weights"],
            } for fs in frames]
        }
        return {"store": store, "frames_payload": frames_payload}

    #%%% 第二层：两个 analyse 并行（small → 两批）
    @task
    @timing
    def analyse_0(prev):
        store = prev["store"]
        event = prev["frames_payload"]["frames"][0]
        tmp_dir = "/tmp"
        frames = list(load_frames(store, event["benchmark_bucket"], event["frames_bucket"], event["frames"], tmp_dir))
        net = load_model(store, event["benchmark_bucket"], event["model_bucket"] + '/' + event["model_weights"], event["model_bucket"] + '/' + event["model_config"], tmp_dir)
        preds = [detect(net, frame) for frame in frames]
        frames_names = [x.split(".")[0] for x in event["frames"]]
        preds = {f"{frames_names[idx]}": dets for idx, dets in enumerate(preds)}
        return {"store": store, "preds": preds}

    @task
    @timing
    def analyse_1(prev):
        store = prev["store"]
        event = prev["frames_payload"]["frames"][1]
        tmp_dir = "/tmp"
        frames = list(load_frames(store, event["benchmark_bucket"], event["frames_bucket"], event["frames"], tmp_dir))
        net = load_model(store, event["benchmark_bucket"], event["model_bucket"] + '/' + event["model_weights"], event["model_bucket"] + '/' + event["model_config"], tmp_dir)
        preds = [detect(net, frame) for frame in frames]
        frames_names = [x.split(".")[0] for x in event["frames"]]
        preds = {f"{frames_names[idx]}": dets for idx, dets in enumerate(preds)}
        return {"store": store, "preds": preds}

    #%%% 第三层：summarize（汇总两个 analyse 的结果）
    @task
    @timing
    def summarize_task(a0, a1):
        logs = summarize_handler({"frames": [a0["preds"], a1["preds"]]})
        return logs

    # %%% specify data flow（新的三层流水）
    layer1 = input_and_decode()
    a0 = analyse_0(layer1)
    a1 = analyse_1(layer1)
    summarize_task(a0, a1)

# execute dag
etl_dag = benchmark_w2_d3()
