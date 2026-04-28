"""NVIDIA NIM 在线推理客户端。

封装 OpenAI 兼容的 chat/completions 接口，仅依赖标准库（urllib + json），
不引入 openai SDK 以避免增加 requirements。

API key 优先级：
1. 环境变量 NVIDIA_API_KEY
2. SQLite app_config 中 key="nvidia_api_key"

用法：
    client = NvidiaNimClient()
    text = client.chat(
        messages=[{"role": "user", "content": "..."}],
        model="qwen/qwen2.5-72b-instruct",
        temperature=0.2,
    )
"""
from __future__ import annotations

import json
import os
import socket
import ssl
import urllib.error
import urllib.request
from typing import Any, Dict, List, Optional

from stock_logger import get_logger
from stock_store import load_app_config, save_app_config

logger = get_logger(__name__)

NIM_BASE_URL = "https://integrate.api.nvidia.com/v1"
NIM_API_KEY_CONFIG_KEY = "nvidia_api_key"

# 默认模型：Qwen3 系列对中文金融文本理解最佳；可在 GUI 设置或调用时覆盖
DEFAULT_MODEL = "qwen/qwen3-next-80b-a3b-instruct"
FALLBACK_MODEL = "meta/llama-3.3-70b-instruct"


class LlmConfigError(Exception):
    """API key 缺失或配置错误。"""


class LlmRequestError(Exception):
    """请求失败（网络/HTTP/JSON 解析）。"""


def _resolve_api_key(explicit_key: Optional[str] = None) -> str:
    if explicit_key and str(explicit_key).strip():
        return str(explicit_key).strip()
    env_key = os.environ.get("NVIDIA_API_KEY", "").strip()
    if env_key:
        return env_key
    cfg_key = load_app_config(NIM_API_KEY_CONFIG_KEY, default="")
    if isinstance(cfg_key, str) and cfg_key.strip():
        return cfg_key.strip()
    raise LlmConfigError(
        "未配置 NVIDIA_API_KEY。请设置环境变量 NVIDIA_API_KEY，"
        "或在应用设置中保存 NIM API Key。"
    )


def save_api_key(api_key: str) -> None:
    """把 API key 持久化到 app_config（只在用户主动保存时调用）。"""
    save_app_config(NIM_API_KEY_CONFIG_KEY, str(api_key or "").strip())


def has_api_key() -> bool:
    try:
        _resolve_api_key()
        return True
    except LlmConfigError:
        return False


class NvidiaNimClient:
    def __init__(
        self,
        api_key: Optional[str] = None,
        base_url: str = NIM_BASE_URL,
        timeout: float = 60.0,
    ) -> None:
        self._explicit_key = api_key
        self._base_url = base_url.rstrip("/")
        self._timeout = float(timeout)
        self._ssl_ctx = ssl.create_default_context()

    def chat(
        self,
        messages: List[Dict[str, str]],
        *,
        model: str = DEFAULT_MODEL,
        temperature: float = 0.2,
        top_p: float = 0.9,
        max_tokens: int = 1500,
        response_format: Optional[Dict[str, Any]] = None,
        extra_payload: Optional[Dict[str, Any]] = None,
    ) -> str:
        """同步调用 chat/completions，返回 assistant 文本内容。

        失败抛 LlmRequestError；API key 缺失抛 LlmConfigError。
        """
        api_key = _resolve_api_key(self._explicit_key)

        payload: Dict[str, Any] = {
            "model": model,
            "messages": messages,
            "temperature": float(temperature),
            "top_p": float(top_p),
            "max_tokens": int(max_tokens),
            "stream": False,
        }
        if response_format:
            payload["response_format"] = response_format
        if extra_payload:
            payload.update(extra_payload)

        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        req = urllib.request.Request(
            f"{self._base_url}/chat/completions",
            data=body,
            method="POST",
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
                "Accept": "application/json",
            },
        )
        try:
            with urllib.request.urlopen(
                req, timeout=self._timeout, context=self._ssl_ctx
            ) as resp:
                raw = resp.read().decode("utf-8", errors="replace")
        except urllib.error.HTTPError as e:
            err_body = ""
            try:
                err_body = e.read().decode("utf-8", errors="replace")
            except Exception:
                pass
            raise LlmRequestError(
                f"NIM HTTP {e.code}: {e.reason} — {err_body[:400]}"
            ) from e
        except (urllib.error.URLError, socket.timeout, TimeoutError) as e:
            raise LlmRequestError(f"NIM 请求失败: {e}") from e

        try:
            data = json.loads(raw)
        except ValueError as e:
            raise LlmRequestError(f"NIM 响应非合法 JSON: {raw[:400]}") from e

        try:
            return str(data["choices"][0]["message"]["content"] or "")
        except (KeyError, IndexError, TypeError) as e:
            raise LlmRequestError(f"NIM 响应缺少 choices/message: {raw[:400]}") from e
