import os
import io
import copy

# %%% 说明：
# %%% 这个模块用一个 Python 字典来模拟“远端对象存储”，并且所有对“存储”的操作
# %%% 都是纯函数：不依赖全局可变状态，不产生副作用。每个函数接收 store(dict)
# %%% 并返回（必要时）更新后的新 store，这样你的业务逻辑可以在 Airflow/Knative
# %%% 的任务之间以参数的方式传递 store，实现可复现、可测试的纯函数数据流。


# %%% 初始化一个空的存储
def init_store() -> dict:
    return {}


# %%% 规范化 bucket 和 key，内部统一以 store[bucket][key] 存放字节串
def _ensure_bucket(store: dict, bucket: str) -> dict:
    if bucket not in store:
        new_store = copy.deepcopy(store)
        new_store[bucket] = {}
        return new_store
    return store


# %%% 写入字节：返回“新 store”与写入的完整对象路径（bucket/key）
def put_bytes(store: dict, bucket: str, key: str, data: bytes) -> tuple[dict, str]:
    store = _ensure_bucket(store, bucket)
    new_store = copy.deepcopy(store)
    new_store[bucket][key] = bytes(data)
    return new_store, f"{bucket}/{key}"


# %%% 从本地文件写入：读取文件字节后调用 put_bytes
def upload(store: dict, bucket: str, key: str, local_path: str) -> tuple[dict, str]:
    with open(local_path, "rb") as f:
        data = f.read()
    return put_bytes(store, bucket, key, data)


# %%% 以字节流（BytesIO）写入
def upload_stream(store: dict, bucket: str, key: str, stream: io.BytesIO) -> tuple[dict, str]:
    stream.seek(0)
    data = stream.read()
    return put_bytes(store, bucket, key, data)


# %%% 读取字节（不会修改 store）
def get_bytes(store: dict, bucket: str, key: str) -> bytes:
    return store[bucket][key]


# %%% 下载到本地文件（不会修改 store）
def download(store: dict, bucket: str, key: str, dest_path: str) -> None:
    data = get_bytes(store, bucket, key)
    os.makedirs(os.path.dirname(dest_path), exist_ok=True)
    with open(dest_path, "wb") as f:
        f.write(data)


# %%% 以 BytesIO 形式读取（不会修改 store）
def download_stream(store: dict, bucket: str, key: str) -> io.BytesIO:
    return io.BytesIO(get_bytes(store, bucket, key))


# %%% 按“前缀”列出对象 key 列表（不含 bucket 前缀）
def list_directory(store: dict, bucket: str, prefix: str) -> list[str]:
    if bucket not in store:
        return []
    return [key for key in store[bucket].keys() if key.startswith(prefix)]


