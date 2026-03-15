import json
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any, Union

class ConfigManager:
    def __init__(self, config: dict, logger, data_dir: Path):
        self.logger = logger
        self.config = config or {}
        self.is_alive = True
        
        # 接收外部传入的目录路径
        self.data_dir = data_dir
        self.data_file = self.data_dir / "data.json"
        self.config_path = self.data_dir / "config.json"
        
        try:
            self.data_dir.mkdir(parents=True, exist_ok=True)
        except Exception as e:
            self.logger.error(f"无法创建插件数据目录: {e}")
            
        # 共享状态缓存
        self.member_cache = {}
        self.realtime_activity = {} 
        self.warned_users: Dict[str, int] = {}
        self.punish_counts: Dict[str, int] = {} 
        self.punish_times: Dict[str, int] = {} 
        self.enforce_mutes: Dict[str, int] = {} 
        self.custom_welcome = {}
        self.event_dedup_cache = {} 
        self.last_check_time = 0
        self.blacklist = []

        self.load_config_safe()
        self.save_config()
        self.load_data()
        self.blacklist = self.parse_list("security_config", "black_list")

    # ==================== 核心配置工具 ====================
    def get_cfg(self, section: str, key: str, default: Any = None) -> Any:
        return self.config.get(section, {}).get(key, default)

    def get_bool(self, section: str, key: str, default: bool = False) -> bool:
        val = self.get_cfg(section, key, default)
        if isinstance(val, bool): return val
        if isinstance(val, str): return val.lower() in ['true', '1', 'yes', 'on']
        if isinstance(val, int): return val == 1
        return default

    def set_cfg(self, section: str, key: str, value: Any):
        if section not in self.config: 
            self.config[section] = {}
        self.config[section][key] = value
        self.save_config()

    def parse_list(self, section: str, key: str) -> List[int]:
        val = self.get_cfg(section, key, [])
        if isinstance(val, list): 
            return [int(x) for x in val if str(x).isdigit()]
        if isinstance(val, str):
            if not val.strip(): return []
            return [int(x.strip()) for x in val.split(',') if x.strip().isdigit()]
        return []

    # ==================== 时间格式转换工具 ====================
    def ts_to_str(self, ts: Union[int, float]) -> str:
        if not ts: return ""
        try: return datetime.fromtimestamp(float(ts)).strftime("%Y-%m-%d %H:%M:%S")
        except Exception: return ""

    def str_to_ts(self, date_str: str) -> int:
        if not date_str or not isinstance(date_str, str): return 0
        try: return int(datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S").timestamp())
        except Exception: return 0

    # ==================== 配置与数据存取 ====================
    def load_config_safe(self):
        try: 
            if self.config_path.exists():
                with open(self.config_path, 'r', encoding='utf-8-sig') as f: 
                    local_data = json.load(f)
                    if not isinstance(local_data, dict): return
                    dynamic_keys = ['black_list', 'whitelist', 'monitored_groups', 'admin_list', 'black_groups']
                    for section, content in local_data.items():
                        if section not in self.config: self.config[section] = {}
                        if isinstance(content, dict):
                            for k, v in content.items():
                                if k in dynamic_keys and k not in self.config[section]:
                                    self.config[section][k] = v
        except Exception as e: 
            self.logger.warning(f"读取本地配置出错: {e}")

    def save_config(self):
        try: 
            self.config_path.parent.mkdir(parents=True, exist_ok=True)
            with open(self.config_path, 'w', encoding='utf-8') as f: 
                json.dump(self.config, f, ensure_ascii=False, indent=2)
        except Exception: pass

    def load_data(self):
        if self.data_file.exists():
            try:
                with open(self.data_file, 'r', encoding='utf-8') as f:
                    d = json.load(f)
                    self.custom_welcome = d.get('custom_welcome', {})
                    self.punish_counts = d.get('punish_counts', {}) 
                    
                    self.warned_users = {k: self.str_to_ts(v) for k, v in d.get('warned_users', {}).items() if isinstance(v, str)}
                    
                    self.realtime_activity = {}
                    for gid, users in d.get('realtime_activity', {}).items():
                        self.realtime_activity[gid] = {uid: self.str_to_ts(ts) for uid, ts in users.items() if isinstance(ts, str)}

                    self.punish_times = {k: self.str_to_ts(v) for k, v in d.get('punish_times', {}).items() if isinstance(v, str)}
                            
                    current_time = int(time.time())
                    self.enforce_mutes = {}
                    for k, val in d.get('enforce_mutes', {}).items():
                        ts = self.str_to_ts(val) if isinstance(val, str) else int(val)
                        if ts > current_time: self.enforce_mutes[k] = ts

                    raw_time = d.get('last_check_time', "")
                    self.last_check_time = self.str_to_ts(raw_time) if isinstance(raw_time, str) else 0
            except Exception as e: 
                self.logger.error(f"加载数据失败: {e}")

    def save_data(self):
        try:
            save_activity = {gid: {uid: self.ts_to_str(ts) for uid, ts in users.items()} for gid, users in self.realtime_activity.items()}
            save_punish = {k: self.ts_to_str(ts) for k, ts in self.punish_times.items()}
            save_warned = {k: self.ts_to_str(ts) for k, ts in self.warned_users.items()}

            current_time = int(time.time())
            keys_to_delete = []
            save_enforce = {}
            for k, ts in self.enforce_mutes.items():
                if current_time >= ts: keys_to_delete.append(k)
                else: save_enforce[k] = self.ts_to_str(ts)
            for k in keys_to_delete: del self.enforce_mutes[k]

            d = {
                'warned_users': save_warned, 
                'realtime_activity': save_activity,
                'custom_welcome': self.custom_welcome,
                'punish_counts': self.punish_counts,
                'punish_times': save_punish,
                'enforce_mutes': save_enforce,  
                'last_check_time': self.ts_to_str(self.last_check_time)
            }
            with open(self.data_file, 'w', encoding='utf-8') as f: 
                json.dump(d, f, ensure_ascii=False, indent=2)
        except Exception as e:
            self.logger.error(f"保存数据失败: {e}")
            