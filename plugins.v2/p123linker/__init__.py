import ast
import sys
import time
import hashlib
import random
from urllib.parse import quote, unquote
from collections import deque
from datetime import datetime, timedelta
from typing import Any, List, Dict, Tuple, Optional
from pathlib import Path

import pytz
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from fastapi import Request
from fastapi.responses import JSONResponse, RedirectResponse
import requests
from cachetools import cached, TTLCache
from p123 import P123Client, check_response

from app.chain.storage import StorageChain
from app.core.config import settings
from app.core.event import eventmanager, Event
from app.log import logger
from app.plugins import _PluginBase
from app.schemas import TransferInfo, FileItem, RefreshMediaItem, ServiceInfo
from app.core.context import MediaInfo
from app.helper.mediaserver import MediaServerHelper
from app.schemas.types import EventType
from app.utils.system import SystemUtils


def iterdir(
    client: P123Client,
    parent_id: int = 0,
    interval: int | float = 0,
    only_file: bool = False,
):
    """
    遍历文件列表
    广度优先搜索
    return:
        迭代器
    Yields:
        dict: 文件或目录信息（包含路径）
    """
    queue = deque()
    queue.append((parent_id, ""))
    while queue:
        current_parent_id, current_path = queue.popleft()
        page = 1
        _next = 0
        while True:
            payload = {
                "limit": 100,
                "next": _next,
                "Page": page,
                "parentFileId": current_parent_id,
                "inDirectSpace": "false",
            }
            if interval != 0:
                time.sleep(interval)
            resp = client.fs_list(payload)
            check_response(resp)
            item_list = resp.get("data").get("InfoList")
            if not item_list:
                break
            for item in item_list:
                is_dir = bool(item["Type"])
                item_path = (
                    f"{current_path}/{item['FileName']}"
                    if current_path
                    else item["FileName"]
                )
                if is_dir:
                    queue.append((int(item["FileId"]), item_path))
                if is_dir and only_file:
                    continue
                if is_dir:
                    yield {
                        **item,
                        "relpath": item_path,
                        "is_dir": is_dir,
                        "id": int(item["FileId"]),
                        "parent_id": int(current_parent_id),
                        "name": item["FileName"],
                    }
                else:
                    yield {
                        **item,
                        "relpath": item_path,
                        "is_dir": is_dir,
                        "id": int(item["FileId"]),
                        "parent_id": int(current_parent_id),
                        "name": item["FileName"],
                        "size": int(item["Size"]),
                        "md5": item["Etag"],
                        "uri": f"123://{quote(item['FileName'])}|{int(item['Size'])}|{item['Etag']}?{item['S3KeyFlag']}",
                    }
            if resp.get("data").get("Next") == "-1":
                break
            else:
                page += 1
                _next = resp.get("data").get("Next")


class FullSyncStrmHelper:
    """
    全量生成 STRM 文件
    """

    def __init__(
        self,
        client,
        user_rmt_mediaext: str,
        user_download_mediaext: str,
        server_address: str,
        storagechain,
        auto_download_mediainfo: bool = False,
    ):
        self.rmt_mediaext = [
            f".{ext.strip()}" for ext in user_rmt_mediaext.replace("，", ",").split(",")
        ]
        self.download_mediaext = [
            f".{ext.strip()}"
            for ext in user_download_mediaext.replace("，", ",").split(",")
        ]
        self.auto_download_mediainfo = auto_download_mediainfo
        self.client = client
        self.strm_count = 0
        self.mediainfo_count = 0
        self.server_address = server_address.rstrip("/")
        self._storagechain = storagechain

    def generate_strm_files(self, full_sync_strm_paths):
        """
        生成 STRM 文件
        """
        media_paths = full_sync_strm_paths.split("\n")
        for path in media_paths:
            if not path:
                continue
            parts = path.split("#", 1)
            pan_media_dir = parts[1]
            target_dir = parts[0]

            try:
                fileitem = self._storagechain.get_file_item(
                    storage="123云盘", path=Path(pan_media_dir)
                )
                parent_id = int(fileitem.fileid)
                logger.info(f"【全量STRM生成】网盘媒体目录 ID 获取成功: {parent_id}")
            except Exception as e:
                logger.error(f"【全量STRM生成】网盘媒体目录 ID 获取失败: {e}")
                return False

            try:
                for item in iterdir(
                    client=self.client, parent_id=parent_id, interval=1, only_file=True
                ):
                    file_path = pan_media_dir + "/" + item["relpath"]
                    file_path = Path(target_dir) / Path(file_path).relative_to(
                        pan_media_dir
                    )
                    file_target_dir = file_path.parent
                    original_file_name = file_path.name
                    file_name = file_path.stem + ".strm"
                    new_file_path = file_target_dir / file_name

                    if self.auto_download_mediainfo:
                        if file_path.suffix in self.download_mediaext:
                            payload = {
                                "Etag": item["Etag"],
                                "FileID": int(item["FileId"]),
                                "FileName": item["FileName"],
                                "S3KeyFlag": item["S3KeyFlag"],
                                "Size": int(item["Size"]),
                            }
                            resp = self.client.download_info(
                                payload,
                                base_url="",
                                async_=False,
                                headers={"User-Agent": settings.USER_AGENT},
                            )
                            check_response(resp)
                            download_url = resp["data"]["DownloadUrl"]

                            if not download_url:
                                logger.error(
                                    f"【全量STRM生成】{original_file_name} 下载链接获取失败，无法下载该文件"
                                )
                                continue

                            file_path.parent.mkdir(parents=True, exist_ok=True)

                            with requests.get(
                                download_url,
                                stream=True,
                                timeout=30,
                                headers={
                                    "User-Agent": settings.USER_AGENT,
                                },
                            ) as response:
                                response.raise_for_status()
                                with open(file_path, "wb") as f:
                                    for chunk in response.iter_content(chunk_size=8192):
                                        f.write(chunk)

                            logger.info(
                                f"【全量STRM生成】保存 {original_file_name} 文件成功: {file_path}"
                            )
                            self.mediainfo_count += 1
                            continue

                    if file_path.suffix not in self.rmt_mediaext:
                        logger.warn(
                            "【全量STRM生成】跳过网盘路径: %s",
                            str(file_path).replace(str(target_dir), "", 1),
                        )
                        continue

                    new_file_path.parent.mkdir(parents=True, exist_ok=True)

                    strm_url = f"{self.server_address}/api/v1/plugin/p123linker/redirect_url?apikey={settings.API_TOKEN}&name={item['FileName']}&size={item['Size']}&md5={item['Etag']}&s3_key_flag={item['S3KeyFlag']}"

                    with open(new_file_path, "w", encoding="utf-8") as file:
                        file.write(strm_url)
                    self.strm_count += 1
                    logger.info(
                        "【全量STRM生成】生成 STRM 文件成功: %s", str(new_file_path)
                    )
            except Exception as e:
                logger.error(f"【全量STRM生成】全量生成 STRM 文件失败: {e}")
                return False
        logger.info(
            f"【全量STRM生成】全量生成 STRM 文件完成，总共生成 {self.strm_count} 个 STRM 文件，下载 {self.mediainfo_count} 个元数据"
        )
        return True


class p123linker(_PluginBase):
    # 插件名称
    plugin_name = "123云盘直链STRM"
    # 插件描述
    plugin_desc = "生成123云盘直链STRM"
    # 插件图标
    plugin_icon = "https://raw.githubusercontent.com/BenBeartop/MoviePilot-Plugins/main/icons/123pan.png"
    # 插件版本
    plugin_version = "1.0.0"
    # 插件作者
    plugin_author = "BenBear"
    # 作者主页
    author_url = "https://github.com/BenBeartop"
    # 插件配置项ID前缀
    plugin_config_prefix = "p123linker_"
    # 加载顺序
    plugin_order = 99
    # 可使用的用户级别
    auth_level = 1

    # 私有属性
    _storagechain = None
    _client = None
    _scheduler = None
    _enabled = False
    _once_full_sync_strm = False
    _once_incremental_sync = False
    _passport = None
    _password = None
    _user_rmt_mediaext = None
    _user_download_mediaext = None
    _timing_full_sync_strm = False
    _full_sync_auto_download_mediainfo_enabled = False
    _cron_full_sync_strm = None
    _full_sync_strm_paths = None
    _uid = None
    _secret_key = None
    _expire_time = 10 * 365 * 24 * 60  # 10年（分钟）
    _custom_domain = "https://vip.123pan.cn"
    _last_sync_time = 0  # 上次同步时间戳
    _incremental_sync_interval = 30  # 增量同步间隔（分钟）
    _incremental_sync_enabled = False

    @staticmethod
    def iterdir(
        client: P123Client,
        parent_id: int = 0,
        interval: int | float = 0,
        only_file: bool = False,
    ):
        """
        遍历文件列表
        广度优先搜索
        return:
            迭代器
        Yields:
            dict: 文件或目录信息（包含路径）
        """
        queue = deque()
        queue.append((parent_id, ""))
        while queue:
            current_parent_id, current_path = queue.popleft()
            page = 1
            _next = 0
            while True:
                payload = {
                    "limit": 100,
                    "next": _next,
                    "Page": page,
                    "parentFileId": current_parent_id,
                    "inDirectSpace": "false",
                }
                if interval != 0:
                    time.sleep(interval)
                resp = client.fs_list(payload)
                check_response(resp)
                item_list = resp.get("data").get("InfoList")
                if not item_list:
                    break
                for item in item_list:
                    is_dir = bool(item["Type"])
                    item_path = (
                        f"{current_path}/{item['FileName']}"
                        if current_path
                        else item["FileName"]
                    )
                    if is_dir:
                        queue.append((int(item["FileId"]), item_path))
                    if is_dir and only_file:
                        continue
                    if is_dir:
                        yield {
                            **item,
                            "relpath": item_path,
                            "is_dir": is_dir,
                            "id": int(item["FileId"]),
                            "parent_id": int(current_parent_id),
                            "name": item["FileName"],
                        }
                    else:
                        yield {
                            **item,
                            "relpath": item_path,
                            "is_dir": is_dir,
                            "id": int(item["FileId"]),
                            "parent_id": int(current_parent_id),
                            "name": item["FileName"],
                            "size": int(item["Size"]),
                            "md5": item["Etag"],
                            "uri": f"123://{quote(item['FileName'])}|{int(item['Size'])}|{item['Etag']}?{item['S3KeyFlag']}",
                        }
                if resp.get("data").get("Next") == "-1":
                    break
                else:
                    page += 1
                    _next = resp.get("data").get("Next")

    def init_plugin(self, config: dict = None):
        """
        初始化插件
        """
        logger.info("【123云盘直链】开始初始化插件...")
        
        self._storagechain = StorageChain()
        # 设置默认值：链接有效期10年，默认域名vip.123pan.cn
        self._expire_time = 10 * 365 * 24 * 60  # 10年（分钟）
        self._custom_domain = "https://vip.123pan.cn"
        self._last_sync_time = int(time.time())  # 初始化为当前时间
        self._incremental_sync_interval = 30  # 增量同步间隔（分钟）
        self._once_incremental_sync = False  # 立刻增量同步

        if config:
            logger.info("【123云盘直链】加载配置...")
            self._enabled = config.get("enabled")
            self._once_full_sync_strm = config.get("once_full_sync_strm")
            self._once_incremental_sync = config.get("once_incremental_sync")
            self._passport = config.get("passport")
            self._password = config.get("password")
            self._uid = config.get("uid")
            self._secret_key = config.get("secret_key")
            self._expire_time = int(config.get("expire_time") or 10 * 365 * 24 * 60)
            self._custom_domain = config.get("custom_domain") or "https://vip.123pan.cn"
            self._user_rmt_mediaext = config.get("user_rmt_mediaext")
            self._user_download_mediaext = config.get("user_download_mediaext")
            self._timing_full_sync_strm = config.get("timing_full_sync_strm")
            self._full_sync_auto_download_mediainfo_enabled = config.get("full_sync_auto_download_mediainfo_enabled")
            self._cron_full_sync_strm = config.get("cron_full_sync_strm")
            self._full_sync_strm_paths = config.get("full_sync_strm_paths")
            # 如果配置中有上次同步时间则使用，否则使用当前时间
            saved_time = config.get("last_sync_time")
            self._last_sync_time = int(saved_time) if saved_time else int(time.time())
            self._incremental_sync_interval = int(config.get("incremental_sync_interval") or 30)
            self._incremental_sync_enabled = config.get("incremental_sync_enabled") or False

            if not self._user_rmt_mediaext:
                self._user_rmt_mediaext = "mp4,mkv,ts,iso,rmvb,avi,mov,mpeg,mpg,wmv,3gp,asf,m4v,flv,m2ts,tp,f4v"
            if not self._user_download_mediaext:
                self._user_download_mediaext = "nfo,jpg,jpeg,png,svg,ass,srt,sup,mp3,flac,wav,aac"
            if not self._cron_full_sync_strm:
                self._cron_full_sync_strm = "0 */7 * * *"
            self.__update_config()

        if self.__check_python_version() is False:
            self._enabled, self._once_full_sync_strm = False, False
            self.__update_config()
            return False

        try:
            logger.info("【123云盘直链】初始化123云盘客户端...")
            self._client = P123Client(self._passport, self._password)
            logger.info("【123云盘直链】123云盘客户端初始化成功")
        except Exception as e:
            logger.error(f"【123云盘直链】123云盘客户端创建失败: {e}")

        # 停止现有任务
        self.stop_service()

        if self._enabled and self._once_full_sync_strm:
            logger.info("【123云盘直链】准备执行立即全量同步...")
            self._scheduler = BackgroundScheduler(timezone=settings.TZ)
            self._scheduler.add_job(
                func=self.full_sync_strm_files,
                trigger="date",
                run_date=datetime.now(tz=pytz.timezone(settings.TZ))
                + timedelta(seconds=3),
                name="123云盘助手立刻全量同步",
            )
            self._once_full_sync_strm = False
            self.__update_config()
            if self._scheduler.get_jobs():
                self._scheduler.print_jobs()
                self._scheduler.start()
                logger.info("【123云盘直链】立即全量同步任务已启动")

        if self._enabled and self._once_incremental_sync:
            logger.info("【123云盘直链】准备执行立即增量同步...")
            self._scheduler = BackgroundScheduler(timezone=settings.TZ)
            self._scheduler.add_job(
                func=self.incremental_sync_files,
                trigger="date",
                run_date=datetime.now(tz=pytz.timezone(settings.TZ))
                + timedelta(seconds=3),
                name="123云盘助手立刻增量同步",
            )
            self._once_incremental_sync = False
            self.__update_config()
            if self._scheduler.get_jobs():
                self._scheduler.print_jobs()
                self._scheduler.start()
                logger.info("【123云盘直链】立即增量同步任务已启动")

        logger.info("【123云盘直链】插件初始化完成")

    def get_state(self) -> bool:
        return self._enabled

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        pass

    def get_api(self) -> List[Dict[str, Any]]:
        """
        不需要API接口
        """
        return []

    def get_service(self) -> List[Dict[str, Any]]:
        """
        注册插件公共服务
        """
        cron_service = []
        
        # 定期全量同步
        if (
            self._cron_full_sync_strm
            and self._timing_full_sync_strm
            and self._full_sync_strm_paths
        ):
            cron_service.append(
                {
                    "id": "p123linker_full_sync_strm_files",
                    "name": "定期全量同步123媒体库",
                    "trigger": CronTrigger.from_crontab(self._cron_full_sync_strm),
                    "func": self.full_sync_strm_files,
                    "kwargs": {},
                }
            )
            
        # 增量同步
        if self._incremental_sync_enabled and self._full_sync_strm_paths:
            try:
                interval = max(1, int(self._incremental_sync_interval))
                cron_service.append(
                    {
                        "id": "p123linker_incremental_sync",
                        "name": "增量同步123媒体库",
                        "trigger": CronTrigger(
                            minute=f"*/{interval}"
                        ),
                        "func": self.incremental_sync_files,
                        "kwargs": {},
                    }
                )
                logger.info(f"【123云盘直链】增量同步已启用，间隔{interval}分钟")
            except Exception as e:
                logger.error(f"【123云盘直链】增量同步配置错误: {e}")
            
        if cron_service:
            return cron_service

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        """
        拼装插件配置页面，需要返回两块数据：1、页面配置；2、数据结构
        """
        return [
            # 账号设置卡片
            {
                "component": "VCard",
                "props": {
                    "variant": "outlined",
                    "class": "mb-3",
                    "color": "grey-lighten-5",
                    "border": "thin",
                },
                "content": [
                    {
                        "component": "VCardItem",
                        "content": [
                            {
                                "component": "VCardTitle",
                                "props": {"class": "text-h6"},
                                "content": [
                                    {
                                        "component": "VIcon",
                                        "props": {
                                            "icon": "mdi-account-circle",
                                            "start": True,
                                            "color": "success",
                                            "size": "small",
                                        }
                                    },
                                    {"component": "span", "props": {"class": "ml-2"}, "text": "账号设置"},
                                ]
                            }
                        ]
                    },
                    {"component": "VDivider"},
                    {
                        "component": "VCardText",
                        "content": [
                            {
                                "component": "VRow",
                                "content": [
                                    {
                                        "component": "VCol",
                                        "props": {"cols": 12, "md": 3},
                                        "content": [
                                            {
                                                "component": "VSwitch",
                                                "props": {
                                                    "model": "enabled",
                                                    "label": "启用插件",
                                                    "color": "primary",
                                                    "density": "default",
                                                    "hide-details": True,
                                                    "class": "ma-0 pa-0",
                                                    "variant": "flat",
                                                },
                                            }
                                        ],
                                    },
                                    {
                                        "component": "VCol",
                                        "props": {"cols": 12, "md": 3},
                                        "content": [
                                            {
                                                "component": "VTextField",
                                                "props": {
                                                    "model": "passport",
                                                    "label": "手机号",
                                                    "variant": "outlined",
                                                    "density": "compact",
                                                    "color": "success",
                                                },
                                            }
                                        ],
                                    },
                                    {
                                        "component": "VCol",
                                        "props": {"cols": 12, "md": 3},
                                        "content": [
                                            {
                                                "component": "VTextField",
                                                "props": {
                                                    "model": "password",
                                                    "label": "密码",
                                                    "variant": "outlined",
                                                    "density": "compact",
                                                    "color": "success",
                                                },
                                            }
                                        ],
                                    },
                                    {
                                        "component": "VCol",
                                        "props": {"cols": 12, "md": 3},
                                        "content": [
                                            {
                                                "component": "VTextField",
                                                "props": {
                                                    "model": "uid",
                                                    "label": "123云盘用户ID",
                                                    "variant": "outlined",
                                                    "density": "compact",
                                                    "color": "success",
                                                },
                                            }
                                        ],
                                    },
                                ],
                            },
                        ],
                    },
                ],
            },
            # 直链设置卡片
            {
                "component": "VCard",
                "props": {
                    "variant": "outlined",
                    "class": "mb-3",
                    "color": "grey-lighten-5",
                    "border": "thin",
                },
                "content": [
                    {
                        "component": "VCardItem",
                        "content": [
                            {
                                "component": "VCardTitle",
                                "props": {"class": "text-h6"},
                                "content": [
                                    {
                                        "component": "VIcon",
                                        "props": {
                                            "icon": "mdi-link-box",
                                            "start": True,
                                            "color": "warning",
                                            "size": "small",
                                        }
                                    },
                                    {"component": "span", "props": {"class": "ml-2"}, "text": "直链设置"},
                                ]
                            }
                        ]
                    },
                    {"component": "VDivider"},
                    {
                        "component": "VCardText",
                        "content": [
                            {
                                "component": "VRow",
                                "content": [
                                    {
                                        "component": "VCol",
                                        "props": {"cols": 12, "md": 4},
                                        "content": [
                                            {
                                                "component": "VTextField",
                                                "props": {
                                                    "model": "secret_key",
                                                    "label": "鉴权密钥",
                                                    "variant": "outlined",
                                                    "density": "compact",
                                                    "color": "warning",
                                                },
                                            }
                                        ],
                                    },
                                    {
                                        "component": "VCol",
                                        "props": {"cols": 12, "md": 4},
                                        "content": [
                                            {
                                                "component": "VTextField",
                                                "props": {
                                                    "model": "expire_time",
                                                    "label": "链接有效期(分钟)",
                                                    "type": "number",
                                                    "hint": "默认10年",
                                                    "persistent-hint": True,
                                                    "hide-spin-buttons": True,
                                                    "variant": "outlined",
                                                    "density": "compact",
                                                    "color": "warning",
                                                },
                                            }
                                        ],
                                    },
                                    {
                                        "component": "VCol",
                                        "props": {"cols": 12, "md": 4},
                                        "content": [
                                            {
                                                "component": "VTextField",
                                                "props": {
                                                    "model": "custom_domain",
                                                    "label": "自定义域名",
                                                    "placeholder": "例如：pan.example.com",
                                                    "hint": "默认：https://vip.123pan.cn",
                                                    "persistent-hint": True,
                                                    "variant": "outlined",
                                                    "density": "compact",
                                                    "color": "warning",
                                                },
                                            }
                                        ],
                                    },
                                ],
                            },
                        ],
                    },
                ],
            },
            # 文件类型设置卡片
            {
                "component": "VCard",
                "props": {
                    "variant": "outlined",
                    "class": "mb-3",
                    "color": "grey-lighten-5",
                    "border": "thin",
                },
                "content": [
                    {
                        "component": "VCardItem",
                        "content": [
                            {
                                "component": "VCardTitle",
                                "props": {"class": "text-h6"},
                                "content": [
                                    {
                                        "component": "VIcon",
                                        "props": {
                                            "icon": "mdi-file-cog",
                                            "start": True,
                                            "color": "error",
                                            "size": "small",
                                        }
                                    },
                                    {"component": "span", "props": {"class": "ml-2"}, "text": "文件类型设置"},
                                ]
                            }
                        ]
                    },
                    {"component": "VDivider"},
                    {
                        "component": "VCardText",
                        "content": [
                            {
                                "component": "VRow",
                                "content": [
                                    {
                                        "component": "VCol",
                                        "props": {"cols": 12, "md": 6},
                                        "content": [
                                            {
                                                "component": "VTextField",
                                                "props": {
                                                    "model": "user_rmt_mediaext",
                                                    "label": "媒体文件后缀",
                                                    "variant": "outlined",
                                                    "density": "compact",
                                                    "color": "error",
                                                },
                                            }
                                        ],
                                    },
                                    {
                                        "component": "VCol",
                                        "props": {"cols": 12, "md": 6},
                                        "content": [
                                            {
                                                "component": "VTextField",
                                                "props": {
                                                    "model": "user_download_mediaext",
                                                    "label": "元数据后缀",
                                                    "variant": "outlined",
                                                    "density": "compact",
                                                    "color": "error",
                                                },
                                            }
                                        ],
                                    },
                                ],
                            },
                        ],
                    },
                ],
            },
            # 同步设置卡片
            {
                "component": "VCard",
                "props": {
                    "variant": "outlined",
                    "color": "grey-lighten-5",
                    "border": "thin",
                },
                "content": [
                    {
                        "component": "VCardItem",
                        "content": [
                            {
                                "component": "VCardTitle",
                                "props": {"class": "text-h6"},
                                "content": [
                                    {
                                        "component": "VIcon",
                                        "props": {
                                            "icon": "mdi-cloud-sync",
                                            "start": True,
                                            "color": "info",
                                            "size": "small",
                                        }
                                    },
                                    {"component": "span", "props": {"class": "ml-2"}, "text": "同步设置"},
                                ]
                            }
                        ]
                    },
                    {"component": "VDivider"},
                    {
                        "component": "VCardText",
                        "content": [
                            {
                                "component": "VRow",
                                "content": [
                                    {
                                        "component": "VCol",
                                        "props": {"cols": 12},
                                        "content": [
                                            {
                                                "component": "VTextarea",
                                                "props": {
                                                    "model": "full_sync_strm_paths",
                                                    "label": "同步目录",
                                                    "rows": 3,
                                                    
                                                    "hint": "全量扫描，并在对应目录生成STRM。",
                                                    "persistent-hint": True,
                                                    "variant": "outlined",
                                                    "density": "compact",
                                                    "color": "info",
                                                    "bg-color": "white",
                                                },
                                            },
                                            {
                                                "component": "VAlert",
                                                "props": {
                                                    "type": "info",
                                                    "variant": "tonal",
                                                    "density": "compact",
                                                    "class": "mt-2",
                                                },
                                                "content": [
                                                    {
                                                        "component": "div",
                                                        "text": "格式：本地路径#网盘路径"
                                                    },
                                                    {
                                                        "component": "div",
                                                        "props": {"class": "mt-1"},
                                                        "text": "示例：/volume1/strm/movies#/媒体库/电影"
                                                    },
                                                ],
                                            }
                                        ],
                                    },
                                ],
                            },
                            {
                                "component": "VRow",
                                "props": {"class": "mt-2"},
                                "content": [
                                    {
                                        "component": "VCol",
                                        "props": {"cols": 12, "md": 3},
                                        "content": [
                                            {
                                                "component": "VSwitch",
                                                "props": {
                                                    "model": "once_full_sync_strm",
                                                    "label": "立刻全量同步",
                                                    "color": "primary",
                                                    "density": "default",
                                                    "hide-details": True,
                                                    "class": "ma-0 pa-0",
                                                    "variant": "flat",
                                                },
                                            }
                                        ],
                                    },
                                    {
                                        "component": "VCol",
                                        "props": {"cols": 12, "md": 3},
                                        "content": [
                                            {
                                                "component": "VSwitch",
                                                "props": {
                                                    "model": "timing_full_sync_strm",
                                                    "label": "定期全量同步",
                                                    "color": "primary",
                                                    "density": "default",
                                                    "hide-details": True,
                                                    "class": "ma-0 pa-0",
                                                    "variant": "flat",
                                                },
                                            }
                                        ],
                                    },
                                    {
                                        "component": "VCol",
                                        "props": {"cols": 12, "md": 3},
                                        "content": [
                                            {
                                                "component": "VCronField",
                                                "props": {
                                                    "model": "cron_full_sync_strm",
                                                    "label": "全量同步周期",
                                                    "variant": "outlined",
                                                    "density": "compact",
                                                    "color": "grey-lighten-1",
                                                },
                                            }
                                        ],
                                    },
                                    {
                                        "component": "VCol",
                                        "props": {"cols": 12, "md": 3},
                                        "content": [
                                            {
                                                "component": "VSwitch",
                                                "props": {
                                                    "model": "full_sync_auto_download_mediainfo_enabled",
                                                    "label": "下载元数据",
                                                    "color": "primary",
                                                    "density": "default",
                                                    "hide-details": True,
                                                    "class": "ma-0 pa-0",
                                                    "variant": "flat",
                                                },
                                            }
                                        ],
                                    },
                                ],
                            },
                            {
                                "component": "VRow",
                                "props": {"class": "mt-2"},
                                "content": [
                                    {
                                        "component": "VCol",
                                        "props": {"cols": 12, "md": 3},
                                        "content": [
                                            {
                                                "component": "VSwitch",
                                                "props": {
                                                    "model": "once_incremental_sync",
                                                    "label": "立刻增量同步",
                                                    "color": "primary",
                                                    "density": "default",
                                                    "hide-details": True,
                                                    "class": "ma-0 pa-0",
                                                    "variant": "flat",
                                                },
                                            }
                                        ],
                                    },
                                    {
                                        "component": "VCol",
                                        "props": {"cols": 12, "md": 3},
                                        "content": [
                                            {
                                                "component": "VSwitch",
                                                "props": {
                                                    "model": "incremental_sync_enabled",
                                                    "label": "启用增量同步",
                                                    "color": "primary",
                                                    "density": "default",
                                                    "hide-details": True,
                                                    "class": "ma-0 pa-0",
                                                    "variant": "flat",
                                                },
                                            }
                                        ],
                                    },
                                    {
                                        "component": "VCol",
                                        "props": {"cols": 12, "md": 3},
                                        "content": [
                                            {
                                                "component": "VTextField",
                                                "props": {
                                                    "model": "incremental_sync_interval",
                                                    "label": "增量同步间隔(分钟)",
                                                    "type": "number",
                                                    "hint": "默认30分钟",
                                                    "persistent-hint": True,
                                                    "hide-spin-buttons": True,
                                                    "variant": "outlined",
                                                    "density": "compact",
                                                    "color": "grey-lighten-1",
                                                },
                                            }
                                        ],
                                    },
                                ],
                            },
                        ],
                    },
                ],
            },
        ], {
            "enabled": False,
            "once_full_sync_strm": False,
            "once_incremental_sync": False,
            "passport": "",
            "password": "",
            "uid": "",
            "secret_key": "",
            "expire_time": str(10 * 365 * 24 * 60),  # 10年（分钟）
            "custom_domain": "https://vip.123pan.cn",
            "user_rmt_mediaext": "mp4,mkv,ts,iso,rmvb,avi,mov,mpeg,mpg,wmv,3gp,asf,m4v,flv,m2ts,tp,f4v",
            "user_download_mediaext": "nfo,jpg,jpeg,png,svg,ass,srt,sup,mp3,flac,wav,aac",
            "timing_full_sync_strm": False,
            "full_sync_auto_download_mediainfo_enabled": False,
            "cron_full_sync_strm": "0 */7 * * *",
            "full_sync_strm_paths": "",
            "last_sync_time": "0",
            "incremental_sync_interval": "30",
            "incremental_sync_enabled": False,
        }

    def get_page(self) -> List[dict]:
        pass

    def __update_config(self):
        self.update_config(
            {
                "enabled": self._enabled,
                "once_full_sync_strm": self._once_full_sync_strm,
                "once_incremental_sync": self._once_incremental_sync,
                "passport": self._passport,
                "password": self._password,
                "uid": self._uid,
                "secret_key": self._secret_key,
                "expire_time": str(self._expire_time),
                "custom_domain": self._custom_domain,
                "user_rmt_mediaext": self._user_rmt_mediaext,
                "user_download_mediaext": self._user_download_mediaext,
                "timing_full_sync_strm": self._timing_full_sync_strm,
                "full_sync_auto_download_mediainfo_enabled": self._full_sync_auto_download_mediainfo_enabled,
                "cron_full_sync_strm": self._cron_full_sync_strm,
                "full_sync_strm_paths": self._full_sync_strm_paths,
                "last_sync_time": str(self._last_sync_time),
                "incremental_sync_interval": str(self._incremental_sync_interval),
                "incremental_sync_enabled": self._incremental_sync_enabled,
            }
        )

    @staticmethod
    def __check_python_version() -> bool:
        """
        检查Python版本
        """
        if not (sys.version_info.major == 3 and sys.version_info.minor >= 12):
            logger.error(
                "当前MoviePilot使用的Python版本不支持本插件，请升级到Python 3.12及以上的版本使用！"
            )
            return False
        return True

    def has_prefix(self, full_path, prefix_path):
        """
        判断路径是否包含
        """
        full = Path(full_path).parts
        prefix = Path(prefix_path).parts

        if len(prefix) > len(full):
            return False

        return full[: len(prefix)] == prefix

    def __get_media_path(self, paths, media_path):
        """
        获取媒体目录路径
        """
        media_paths = paths.split("\n")
        for path in media_paths:
            if not path:
                continue
            parts = path.split("#", 1)
            if self.has_prefix(media_path, parts[1]):
                return True, parts[0], parts[1]
        return False, None, None

    def _generate_auth_url(self, file_path: str) -> str:
        """
        生成带签名的123云盘直链
        参考Go示例：
        authKey := fmt.Sprintf("%d-%d-%d-%x", ts, rInt, uid, md5.Sum([]byte(fmt.Sprintf("%s-%d-%d-%d-%s",
            objURL.Path, ts, rInt, uid, privateKey))))
        """
        logger.info("【直链生成】开始生成直链...")
        
        if not self._uid or not self._secret_key:
            logger.error("【直链生成】缺少必要的配置：用户ID或鉴权密钥")
            logger.debug(f"【直链生成】当前配置：UID={self._uid}, 密钥长度={len(self._secret_key) if self._secret_key else 0}")
            raise ValueError("缺少必要的配置：用户ID或鉴权密钥")

        if not self._custom_domain:
            logger.error("【直链生成】未配置自定义域名")
            raise ValueError("未配置自定义域名")

        logger.info(f"【直链生成】文件路径: {file_path}")
        
        try:
            # 生成过期时间戳
            expire_time = int(time.time()) + self._expire_time * 60
            # 生成随机数
            rand = random.randint(0, 2**31-1)  # 使用和Go一样的随机数范围
            
            # 构建请求路径
            request_path = f"/{self._uid}/{file_path}"
            # URL解码路径
            request_path = unquote(request_path)
            logger.debug(f"【直链生成】请求路径: {request_path}")
            
            # 构建待签名字符串（注意顺序和格式必须和Go示例完全一致）
            sign_str = f"{request_path}-{expire_time}-{rand}-{self._uid}-{self._secret_key}"
            logger.debug(f"【直链生成】待签名字符串: {sign_str}")
            
            # 计算MD5签名（使用二进制格式）
            sign = hashlib.md5(sign_str.encode()).hexdigest()
            logger.debug(f"【直链生成】MD5签名: {sign}")
            
            # 构建auth_key（格式：timestamp-random-uid-sign）
            auth_key = f"{expire_time}-{rand}-{self._uid}-{sign}"
            
            # 构建基础URL
            domain = self._custom_domain.rstrip('/')
            if not domain.startswith(('http://', 'https://')):
                domain = f"https://{domain}"
            
            base_url = f"{domain}{request_path}"
            
            # 添加auth_key参数
            final_url = f"{base_url}?auth_key={auth_key}"
            logger.info(f"【直链生成】生成直链成功: {final_url}")
            
            return final_url
            
        except Exception as e:
            logger.error(f"【直链生成】生成直链过程中发生错误: {str(e)}")
            raise

    def full_sync_strm_files(self):
        """
        全量同步
        """
        logger.info("【全量同步】开始执行全量同步任务...")
        
        if not self._enabled:
            logger.error("【全量同步】插件未启用，退出同步")
            return

        if not self._full_sync_strm_paths:
            logger.error("【全量同步】未配置同步路径，退出同步")
            return

        if not self._uid or not self._secret_key:
            logger.error("【全量同步】未配置用户ID或鉴权密钥，退出同步")
            return

        try:
            # 准备媒体文件和数据文件扩展名列表
            media_exts = set(f".{ext.strip().lower()}" for ext in self._user_rmt_mediaext.split(','))
            data_exts = set(f".{ext.strip().lower()}" for ext in self._user_download_mediaext.split(','))
            
            logger.info("【全量同步】配置信息：")
            logger.info(f"【全量同步】- 媒体文件后缀: {sorted(list(media_exts))}")
            logger.info(f"【全量同步】- 数据文件扩展名: {sorted(list(data_exts))}")
            logger.info(f"【全量同步】- 自动下载元数据: {self._full_sync_auto_download_mediainfo_enabled}")
            logger.info(f"【全量同步】- 域名: {self._custom_domain}")
            
            paths = self._full_sync_strm_paths.strip().split('\n')
            logger.info(f"【全量同步】待处理路径数量: {len(paths)}")
            
            for path in paths:
                if not path.strip():
                    continue
                
                parts = path.split("#", 1)
                if len(parts) != 2:
                    logger.error(f"【全量同步】路径格式错误: {path}")
                    continue
                    
                local_path, pan_path = parts
                logger.info(f"【全量同步】处理路径对: 本地={local_path}, 网盘={pan_path}")
                
                # 检查本地路径是否存在，如果不存在则创建
                local_dir = Path(local_path)
                if not local_dir.exists():
                    logger.info(f"【全量同步】创建本地目录: {local_path}")
                    local_dir.mkdir(parents=True, exist_ok=True)
                
                # 获取网盘目录ID
                try:
                    # 先列出根目录
                    logger.info("【全量同步】开始获取网盘目录信息...")
                    current_parent_id = 0
                    
                    # 解析路径，移除开头和结尾的斜杠，然后分割
                    pan_path = pan_path.strip('/')
                    if not pan_path:
                        logger.error("【全量同步】网盘路径不能为空")
                        continue
                        
                    pan_path_parts = pan_path.split('/')
                    logger.info(f"【全量同步】需要查找的目录层级: {pan_path_parts}")
                    
                    # 逐级查找目录
                    for part in pan_path_parts:
                        if not part:  # 跳过空字符串
                            continue
                            
                        logger.info(f"【全量同步】查找目录: {part}")
                        found = False
                        
                        # 获取当前层级的目录列表
                        resp = self._client.fs_list({
                            "limit": 100,
                            "next": 0,
                            "parentFileId": current_parent_id,
                            "inDirectSpace": "false",
                        })
                        check_response(resp)
                        
                        # 在当前层级查找目标目录
                        for item in resp.get("data", {}).get("InfoList", []):
                            logger.debug(f"【全量同步】检查目录: {item.get('FileName')} (Type={item.get('Type')})")
                            if item["FileName"] == part and bool(item["Type"]):  # Type=1 表示目录
                                current_parent_id = int(item["FileId"])
                                found = True
                                logger.info(f"【全量同步】找到目录 {part}, ID: {current_parent_id}")
                                break
                                
                        if not found:
                            raise ValueError(f"未找到目录: {part}")
                    
                    parent_id = current_parent_id
                    logger.info(f"【全量同步】成功获取目录ID: {parent_id}")
                    
                    # 开始遍历文件
                    logger.info(f"【全量同步】开始遍历目录下的文件...")
                    for item in self.iterdir(
                        client=self._client,
                        parent_id=parent_id,
                        interval=1,
                        only_file=True
                    ):
                        try:
                            # 检查文件扩展名
                            file_ext = Path(item["FileName"]).suffix.lower()
                            
                            # 构建本地路径
                            file_path = pan_path + "/" + item["relpath"]
                            file_path = Path(local_path) / Path(file_path).relative_to(pan_path)
                            file_target_dir = file_path.parent
                            file_target_dir.mkdir(parents=True, exist_ok=True)
                            
                            if file_ext in media_exts:
                                # 为媒体文件生成STRM
                                original_name = file_path.stem
                                file_name = original_name + ".strm"
                                new_file_path = file_target_dir / file_name
                                
                                # 构建完整的文件路径，确保不以斜杠开头
                                full_path = f"{pan_path}/{item['relpath']}"
                                full_path = full_path.strip('/')
                                # URL编码路径中的特殊字符
                                encoded_path = quote(full_path)
                                
                                url = self._generate_auth_url(encoded_path)
                                
                                with open(new_file_path, "w", encoding="utf-8") as f:
                                    f.write(url)
                                    
                                logger.info(f"【全量同步】生成STRM文件成功: {new_file_path}")
                                
                            elif self._full_sync_auto_download_mediainfo_enabled and file_ext in data_exts:
                                # 下载元数据
                                new_file_path = file_target_dir / item["FileName"]
                                
                                # 获取下载链接
                                resp = self._client.download_info({
                                    "Etag": item["Etag"],
                                    "FileID": int(item["FileId"]),
                                    "FileName": item["FileName"],
                                    "S3KeyFlag": item["S3KeyFlag"],
                                    "Size": int(item["Size"]),
                                })
                                check_response(resp)
                                download_url = resp["data"]["DownloadUrl"]
                                
                                # 下载文件
                                with requests.get(download_url, stream=True) as r:
                                    r.raise_for_status()
                                    with open(new_file_path, 'wb') as f:
                                        for chunk in r.iter_content(chunk_size=8192):
                                            f.write(chunk)
                                            
                                logger.info(f"【全量同步】下载元数据成功: {new_file_path}")
                            else:
                                logger.debug(f"【全量同步】跳过文件: {item['FileName']}")
                                
                        except Exception as e:
                            logger.error(f"【全量同步】处理文件失败: {e}")
                            continue
                            
                except Exception as e:
                    logger.error(f"【全量同步】获取网盘目录ID失败: {e}")
                    continue
            
            logger.info("【全量同步】全量同步任务完成")
            # 全量同步完成后更新时间戳
            self._last_sync_time = int(time.time())
            self.__update_config()
            logger.info(f"【全量同步】更新同步时间戳: {datetime.fromtimestamp(self._last_sync_time)}")
            return True
                
        except Exception as e:
            logger.error(f"【全量同步】执行全量同步时发生错误: {e}")
            return False

    def stop_service(self):
        """
        退出插件
        """
        try:
            if self._scheduler:
                self._scheduler.remove_all_jobs()
                if self._scheduler.running:
                    self._scheduler.shutdown()
                self._scheduler = None
        except Exception as e:
            print(str(e))

    def incremental_sync_files(self):
        """
        增量同步
        """
        logger.info("【增量同步】开始执行增量同步任务...")
        
        if not self._enabled:
            logger.error("【增量同步】插件未启用，退出同步")
            return
            
        if not self._full_sync_strm_paths:
            logger.error("【增量同步】未配置同步路径，退出同步")
            return
            
        if not self._uid or not self._secret_key:
            logger.error("【增量同步】未配置用户ID或鉴权密钥，退出同步")
            return

        # 如果上次同步时间是0，说明还没有进行过同步，先进行一次全量同步
        if self._last_sync_time == 0:
            logger.info("【增量同步】首次运行，执行全量同步...")
            self.full_sync_strm_files()
            return

        try:
            # 准备媒体文件和数据文件扩展名列表
            media_exts = set(f".{ext.strip().lower()}" for ext in self._user_rmt_mediaext.split(','))
            data_exts = set(f".{ext.strip().lower()}" for ext in self._user_download_mediaext.split(','))
            
            logger.info("【增量同步】配置信息：")
            logger.info(f"【增量同步】- 媒体文件后缀: {sorted(list(media_exts))}")
            logger.info(f"【增量同步】- 数据文件扩展名: {sorted(list(data_exts))}")
            logger.info(f"【增量同步】- 自动下载元数据: {self._full_sync_auto_download_mediainfo_enabled}")
            logger.info(f"【增量同步】- 域名: {self._custom_domain}")
            logger.info(f"【增量同步】- 上次同步时间: {datetime.fromtimestamp(self._last_sync_time)}")
            
            paths = self._full_sync_strm_paths.strip().split('\n')
            logger.info(f"【增量同步】待处理路径数量: {len(paths)}")
            
            current_time = int(time.time())
            
            for path in paths:
                if not path.strip():
                    continue
                
                parts = path.split("#", 1)
                if len(parts) != 2:
                    logger.error(f"【增量同步】路径格式错误: {path}")
                    continue
                    
                local_path, pan_path = parts
                logger.info(f"【增量同步】处理路径对: 本地={local_path}, 网盘={pan_path}")
                
                # 检查本地路径是否存在，如果不存在则创建
                local_dir = Path(local_path)
                if not local_dir.exists():
                    logger.info(f"【增量同步】创建本地目录: {local_path}")
                    local_dir.mkdir(parents=True, exist_ok=True)
                
                # 获取网盘目录ID
                try:
                    # 先列出根目录
                    logger.info("【增量同步】开始获取网盘目录信息...")
                    current_parent_id = 0
                    
                    # 解析路径，移除开头和结尾的斜杠，然后分割
                    pan_path = pan_path.strip('/')
                    if not pan_path:
                        logger.error("【增量同步】网盘路径不能为空")
                        continue
                        
                    pan_path_parts = pan_path.split('/')
                    logger.info(f"【增量同步】需要查找的目录层级: {pan_path_parts}")
                    
                    # 逐级查找目录
                    for part in pan_path_parts:
                        if not part:  # 跳过空字符串
                            continue
                            
                        logger.info(f"【增量同步】查找目录: {part}")
                        found = False
                        
                        # 获取当前层级的目录列表
                        resp = self._client.fs_list({
                            "limit": 100,
                            "next": 0,
                            "parentFileId": current_parent_id,
                            "inDirectSpace": "false",
                        })
                        check_response(resp)
                        
                        # 在当前层级查找目标目录
                        for item in resp.get("data", {}).get("InfoList", []):
                            logger.debug(f"【增量同步】检查目录: {item.get('FileName')} (Type={item.get('Type')})")
                            if item["FileName"] == part and bool(item["Type"]):  # Type=1 表示目录
                                current_parent_id = int(item["FileId"])
                                found = True
                                logger.info(f"【增量同步】找到目录 {part}, ID: {current_parent_id}")
                                break
                                
                        if not found:
                            raise ValueError(f"未找到目录: {part}")
                    
                    parent_id = current_parent_id
                    logger.info(f"【增量同步】成功获取目录ID: {parent_id}")
                    
                    # 开始遍历文件
                    logger.info(f"【增量同步】开始遍历目录下的文件...")
                    for item in self.iterdir(
                        client=self._client,
                        parent_id=parent_id,
                        interval=1,
                        only_file=True
                    ):
                        try:
                            # 检查文件修改时间
                            update_time = int(item.get("UpdateTime", 0))
                            if update_time <= self._last_sync_time:
                                logger.debug(f"【增量同步】跳过未修改的文件: {item['FileName']}")
                                continue

                            # 检查文件扩展名
                            file_ext = Path(item["FileName"]).suffix.lower()
                            
                            # 构建本地路径
                            file_path = pan_path + "/" + item["relpath"]
                            file_path = Path(local_path) / Path(file_path).relative_to(pan_path)
                            file_target_dir = file_path.parent
                            file_target_dir.mkdir(parents=True, exist_ok=True)
                            
                            if file_ext in media_exts:
                                # 为媒体文件生成STRM
                                original_name = file_path.stem
                                file_name = original_name + ".strm"
                                new_file_path = file_target_dir / file_name
                                
                                # 构建完整的文件路径，确保不以斜杠开头
                                full_path = f"{pan_path}/{item['relpath']}"
                                full_path = full_path.strip('/')
                                # URL编码路径中的特殊字符
                                encoded_path = quote(full_path)
                                
                                url = self._generate_auth_url(encoded_path)
                                
                                with open(new_file_path, "w", encoding="utf-8") as f:
                                    f.write(url)
                                    
                                logger.info(f"【增量同步】生成STRM文件成功: {new_file_path}")
                                
                            elif self._full_sync_auto_download_mediainfo_enabled and file_ext in data_exts:
                                # 下载元数据
                                new_file_path = file_target_dir / item["FileName"]
                                
                                # 获取下载链接
                                resp = self._client.download_info({
                                    "Etag": item["Etag"],
                                    "FileID": int(item["FileId"]),
                                    "FileName": item["FileName"],
                                    "S3KeyFlag": item["S3KeyFlag"],
                                    "Size": int(item["Size"]),
                                })
                                check_response(resp)
                                download_url = resp["data"]["DownloadUrl"]
                                
                                # 下载文件
                                with requests.get(download_url, stream=True) as r:
                                    r.raise_for_status()
                                    with open(new_file_path, 'wb') as f:
                                        for chunk in r.iter_content(chunk_size=8192):
                                            f.write(chunk)
                                            
                                logger.info(f"【增量同步】下载元数据成功: {new_file_path}")
                            else:
                                logger.debug(f"【增量同步】跳过文件: {item['FileName']}")
                                
                        except Exception as e:
                            logger.error(f"【增量同步】处理文件失败: {e}")
                            continue
                            
                except Exception as e:
                    logger.error(f"【增量同步】获取网盘目录ID失败: {e}")
                    continue
            
            # 更新最后同步时间
            self._last_sync_time = current_time
            self.__update_config()
            logger.info(f"【增量同步】更新同步时间戳: {datetime.fromtimestamp(self._last_sync_time)}")
            
            logger.info("【增量同步】增量同步任务完成")
            return True
                
        except Exception as e:
            logger.error(f"【增量同步】执行增量同步时发生错误: {e}")
            return False
