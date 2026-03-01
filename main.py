import json
import time
import asyncio
import random
import os
import re 
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Union, Any

from astrbot.api.event import AstrMessageEvent, filter
from astrbot.api.star import Context, Star, StarTools
from astrbot.api import logger

class Main(Star):
    def __init__(self, context: Context, config: dict = None) -> None:
        super().__init__(context)

        self.logger = logger
        self.is_alive = True
        self.config = config or {}
        
        self.member_cache = {}
        self.realtime_activity = {}
        self.warned_users: Dict[str, int] = {}
        self.punish_counts: Dict[str, int] = {}
        self.punish_times: Dict[str, int] = {}
        self.enforce_mutes: Dict[str, int] = {}
        self.blacklist = []
        self.custom_welcome = {}
        self.event_dedup_cache = {}
        self.last_check_time = 0
        
        self.plugin_id = "astrbot_plugin_qq_group_manager"
        
        self.data_dir = Path(StarTools.get_data_dir())
        self.data_file = self.data_dir / "data.json"
        self.config_path = Path(StarTools.get_data_dir()) / f"{self.plugin_id}_config.json"
        
        try:
            self.data_dir.mkdir(parents=True, exist_ok=True)
        except Exception as e:
            self.logger.error(f"无法创建插件数据目录: {e}")

        self.lock = asyncio.Lock()
        self._bg_task = None

        try:
            self.load_config_safe()
            self.load_data()

            self.blacklist = self._parse_list_config("security_config", "black_list")

            if not self.config.get("enabled", True):
                self.logger.info("插件已禁用")
                return

            self.logger.info(f"QQ群小管家 已启动")
            self._bg_task = asyncio.create_task(self._background_loop())

        except Exception as e:
            self.logger.error(f"初始化失败: {e}")

    # ==================== 核心配置工具 ====================

    def _get_cfg(self, section: str, key: str, default: Any = None) -> Any:
        return self.config.get(section, {}).get(key, default)

    def _get_bool_cfg(self, section: str, key: str, default: bool = False) -> bool:
        val = self._get_cfg(section, key, default)
        if isinstance(val, bool): return val
        if isinstance(val, str): return val.lower() in ['true', '1', 'yes', 'on']
        if isinstance(val, int): return val == 1
        return default

    async def _set_cfg(self, section: str, key: str, value: Any):
        if section not in self.config:
            self.config[section] = {}
        self.config[section][key] = value
        await self.save_config()

    def _parse_list_config(self, section: str, key: str) -> List[int]:
        val = self._get_cfg(section, key, [])
        if isinstance(val, list):
            return [int(x) for x in val if str(x).isdigit()]
        if isinstance(val, str):
            if not val.strip(): return []
            return [int(x.strip()) for x in val.split(',') if x.strip().isdigit()]
        return []

    # ==================== 时间格式转换工具 ====================

    def _ts_to_str(self, ts: Union[int, float]) -> str:
        if not ts: return ""
        try:
            return datetime.fromtimestamp(float(ts)).strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            return ""

    def _str_to_ts(self, date_str: str) -> int:
        if not date_str or not isinstance(date_str, str): return 0
        try:
            dt = datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")
            return int(dt.timestamp())
        except Exception:
            return 0

    # ==================== 人设读取逻辑 ====================

    async def _get_persona(self):
        try:
            if hasattr(self.context, "persona_manager"):
                pm = self.context.persona_manager
                if hasattr(pm, "personas") and isinstance(pm.personas, list) and len(pm.personas) > 0:
                    p = pm.personas[0]
                    return getattr(p, "system_prompt", None) or getattr(p, "prompt", "")

            provider = self.context.get_using_provider()
            if provider and hasattr(provider, "system_prompt") and provider.system_prompt:
                return provider.system_prompt
        except Exception:
            pass
        return (
            "你是一个高效、客观、偶尔带点毒舌的群管理员。你维护群组秩序，"
            "对违规者严厉，对友好者礼貌。说话简洁明了，不拖泥带水。"
        )

    # ==================== 事件去重与资源 ====================

    def _is_duplicate_event(self, key: str, ttl: int = 5) -> bool:
        current_time = time.time()
        expired_keys = [k for k, v in self.event_dedup_cache.items() if current_time - v >= ttl]
        for k in expired_keys:
            del self.event_dedup_cache[k]
            
        if key in self.event_dedup_cache:
            return True
        self.event_dedup_cache[key] = current_time
        return False

    def _get_img_from_config(self, config_key: str) -> str:
        welcome_conf = self.config.get("welcome_config", {})
        file_list = welcome_conf.get(config_key, [])

        if not file_list or not isinstance(file_list, list):
            return ""

        valid_paths = []
        for path_str in file_list:
            if not path_str or not isinstance(path_str, str):
                continue

            p = Path(path_str)
            if not p.is_absolute():
                try:
                    p = Path(StarTools.get_data_dir()) / path_str
                except Exception:
                    if not p.exists():
                        p = Path.cwd() / path_str

            if p.exists() and p.is_file():
                valid_paths.append(str(p.absolute()))

        if valid_paths:
            return random.choice(valid_paths)

        return ""

    def _get_local_image_cq(self, path: str) -> str:
        if not path: return ""
        try:
            clean_path = path.replace("file:///", "").replace("file://", "")
            abs_path = os.path.abspath(clean_path)
            if os.path.exists(abs_path):
                final_path = abs_path.replace("\\", "/")
                return f"[CQ:image,file=file:///{final_path}]"
        except Exception:
            pass
        return ""

    # ==================== 辅助：获取群成员昵称 ====================
    async def _get_member_name(self, group_id: int, user_id: int, client) -> str:
        try:
            if str(group_id) in self.member_cache:
                members = self.member_cache[str(group_id)].get('members', [])
                for m in members:
                    if int(m.get('user_id', 0)) == user_id:
                        return m.get('card') or m.get('nickname') or str(user_id)
        except Exception:
            pass

        if client:
            try:
                info = await client.call_action('get_group_member_info', group_id=group_id, user_id=user_id, no_cache=False)
                return info.get('card') or info.get('nickname') or str(user_id)
            except Exception:
                pass

            try:
                info = await client.call_action('get_stranger_info', user_id=user_id, no_cache=False)
                return info.get('nickname') or str(user_id)
            except Exception:
                pass

        return str(user_id)

    # ==================== 业务逻辑：加群申请 ====================

    async def on_group_request(self, event):
        if not self.is_alive or not self.config.get("enabled", True):
            return
        try:
            raw_gid = getattr(event, 'group_id', 0)
            user_id = int(getattr(event, 'user_id', 0))
            group_id = int(raw_gid)

            monitored = self._parse_list_config("clean_config", "monitored_groups")
            if group_id not in monitored:
                return

            dedup_key = f"req_{group_id}_{user_id}"
            if self._is_duplicate_event(dedup_key, ttl=5):
                return

            request_type = getattr(event, 'request_type', None)
            sub_type = getattr(event, 'sub_type', None)
            flag = getattr(event, 'flag', str(int(time.time())))

            if request_type == "group" and sub_type == "add":
                comment = getattr(event, 'comment', "") or ""
                self.logger.info(f"收到加群请求: 群[{group_id}] 用户[{user_id}] {comment}")

                client = self._get_qq_client()
                if not client: return

                current_blacklist = self._parse_list_config("security_config", "black_list")
                if user_id in current_blacklist:
                    self.logger.info(f"拒绝黑名单用户 {user_id}")
                    await client.call_action('set_group_add_request', flag=flag, sub_type=sub_type, approve=False, reason="主动退群和黑名单不再同意进群，别申请啦！")
                    return

                if self._get_bool_cfg("approval_config", "auto_approve", False):
                    keywords = self._get_cfg("approval_config", "approval_keywords", [])
                    matched = False
                    if keywords:
                        valid_kws = [k for k in keywords if k]
                        if valid_kws and any(str(k).lower() in comment.lower() for k in valid_kws):
                            matched = True
                    if matched:
                        self.logger.info(f"自动同意 {user_id} 入群")
                        await client.call_action('set_group_add_request', flag=flag, sub_type=sub_type, approve=True)
        except Exception as e:
            self.logger.error(f"处理入群请求出错: {e}")

    # ==================== 业务逻辑：群通知 (迎新/退群/解禁) ====================

    async def on_group_notice(self, event):
        if not self.is_alive or not self.config.get("enabled", True):
            return
        raw_gid = getattr(event, 'group_id', 0)
        try:
            group_id = int(raw_gid)
        except Exception:
            return

        notice_type = getattr(event, 'notice_type', None)
        sub_type = getattr(event, 'sub_type', None)
        user_id = int(getattr(event, 'user_id', 0))
        client = self._get_qq_client()

        if notice_type == "group_ban":
            duration = int(getattr(event, 'duration', 0))
            operator_id = int(getattr(event, 'operator_id', 0))

            self_id = int(getattr(event, 'self_id', 0))
            if self_id == 0 and client:
                try:
                    if hasattr(client, 'self_id'): self_id = int(client.self_id)
                except Exception: pass

            if (sub_type == "lift_ban" or duration == 0) and operator_id != self_id:
                user_key = f"{group_id}_{user_id}"
                current_time = int(time.time())
                
                if user_key in self.enforce_mutes:
                    expire_time = self.enforce_mutes[user_key]
                    
                    if current_time < expire_time:
                        remaining_sec = expire_time - current_time

                        if client:
                            await client.call_action('set_group_ban', group_id=group_id, user_id=user_id, duration=remaining_sec)
                        
                        criminal_name = await self._get_member_name(group_id, user_id, client)
                        
                        llm_text = ""
                        try:
                            provider = self.context.get_using_provider()
                            if provider:
                                persona = await self._get_persona()
                                
                                user_prompt = (
                                    f"【当前人设】\n{persona}\n\n"
                                    f"该群管理员提前解除因辱骂你的罪犯 {criminal_name} 的禁言。\n"
                                    f"你已经光速反制，把那个人重新关回去了。\n"
                                    f"严厉回复这名群管理员不要多管闲事。\n"
                                    f"这个罪犯 {criminal_name} 还没服完刑。\n"
                                    "要求：完全符合你的人设性格说话，真实自然。"
                                )
                                resp = await provider.text_chat(user_prompt, session_id=f"group_{group_id}")
                                llm_text = resp.completion_text if hasattr(resp, 'completion_text') else getattr(resp, 'text', str(resp))
                        except Exception as e:
                            self.logger.warning(f"LLM警告生成失败: {e}")
                            
                        if llm_text:
                            llm_text = re.sub(r'\[CQ:[^\]]+\]', '', llm_text).strip()
                            
                        msg = llm_text if llm_text else f"\n谁允许你擅自解除禁言了？\n“{criminal_name}”骂我的账还没算完！\n劝你不要多管闲事，哼~"
                        if client:
                            await client.call_action('send_group_msg', group_id=group_id, message=f"[CQ:at,qq={operator_id}] \n{msg}")
                    else:
                        del self.enforce_mutes[user_key]
                        await self.save_data() 
            return 

        monitored = self._parse_list_config("clean_config", "monitored_groups")
        if group_id not in monitored:
            return

        black_groups = self._parse_list_config("welcome_config", "black_groups")
        if group_id in black_groups:
            return

        if notice_type == "group_increase":
            dedup_key = f"welcome_{group_id}_{user_id}"
            if self._is_duplicate_event(dedup_key, ttl=5):
                return
            if not self._get_bool_cfg("welcome_config", "enable_welcome", False):
                return
            try:
                default_msg = self._get_cfg("welcome_config", "welcome_msg", "欢迎新成员！")
                welcome_msg = self.custom_welcome.get(str(group_id)) or default_msg
                msg_chain = ""
                if self._get_bool_cfg("welcome_config", "is_at", True):
                    msg_chain += f"[CQ:at,qq={user_id}] "
                welcome_msg = welcome_msg.replace("{nickname}", str(user_id)).replace("{uid}", str(user_id))
                msg_chain += welcome_msg
                
                img_path = self._get_img_from_config("welcome_images_list")
                if img_path:
                    msg_chain += self._get_local_image_cq(img_path)
                
                if client:
                    await asyncio.sleep(1) 
                    await client.call_action('send_group_msg', group_id=group_id, message=msg_chain)
            except Exception as e:
                self.logger.error(f"迎新失败: {e}")

        elif notice_type == "group_decrease":
            dedup_key = f"bye_{group_id}_{user_id}"
            if self._is_duplicate_event(dedup_key, ttl=5):
                return
            if sub_type == "leave" and self._get_bool_cfg("security_config", "kick_black", False):
                try:
                    if not self.is_whitelisted(str(user_id)):
                        current_blacklist = self._parse_list_config("security_config", "black_list")
                        if user_id not in current_blacklist:
                            current_blacklist.append(user_id)
                            await self._set_cfg("security_config", "black_list", current_blacklist)
                            self.blacklist = current_blacklist
                            self.logger.info(f"{user_id} 主动退群自动拉黑")
                except Exception as e:
                    self.logger.error(f"拉黑退群用户失败: {e}")
                    
            if sub_type == "leave" and self._get_bool_cfg("welcome_config", "enable_bye", False):
                try:
                    nickname = str(user_id)
                    try:
                        if client:
                            info = await client.call_action('get_stranger_info', user_id=user_id, no_cache=True)
                            nickname = info.get('nickname', str(user_id))
                    except Exception as e:
                        self.logger.warning(f"获取退群用户信息失败: {e}")
                        
                    bye_tpl = self._get_cfg("welcome_config", "bye_msg", "群友 {username} 离开了。")
                    bye_msg = bye_tpl.format(username=nickname, userid=user_id)
                    
                    img_path = self._get_img_from_config("bye_images_list")
                    if img_path:
                        bye_msg += self._get_local_image_cq(img_path)
                    
                    if client:
                        await client.call_action('send_group_msg', group_id=group_id, message=bye_msg)
                except Exception as e:
                    self.logger.error(f"退群播报失败: {e}")
    
    @filter.event_message_type(filter.EventMessageType.GROUP_MESSAGE)
    async def on_group_message(self, event: AstrMessageEvent):
        try:
            if not self.is_alive:
                return
            group_id = int(event.message_obj.group_id)
            user_id = str(event.get_sender_id())
            
            if not user_id or user_id == "0": 
                return

            monitored = self._parse_list_config("clean_config", "monitored_groups")
            if group_id not in monitored:
                return
            
            gid_str = str(group_id)
            if gid_str not in self.realtime_activity:
                self.realtime_activity[gid_str] = {}
            self.realtime_activity[gid_str][user_id] = int(time.time())
            
            user_key = f"{gid_str}_{user_id}"
            if user_key in self.warned_users:
                self.logger.info(f"监测到 群[{group_id}] [{user_id}] 冒泡了，已解除警告")
                self.warned_users.pop(user_key, None)
                await self.save_data() 
        except Exception as e:
            self.logger.warning(f"处理群消息活动缓存失败: {e}")

    # ==================== LLM 智能惩罚 ====================

    @filter.llm_tool(name="punish_insult_tool")
    async def punish_insult_tool(self, event: AstrMessageEvent, reason: str = "辱骂Bot"):
        """
        当用户对你进行辱骂、人身攻击、说脏话、使用侮辱性词汇时，或者表现出不礼貌或挑衅：即任何对你的能力表示轻视、嘲讽、吐槽（如说你“垃圾”、“废”、“没用”、“菜”）或攻击的言论时，调用此工具对该用户进行禁言惩罚。
        
        Args:
            reason(string): 触发惩罚的具体原因。
        """
        user_id = event.get_sender_id()
        group_id = getattr(event.message_obj, 'group_id', None)
        
        if not group_id:
            return "当前不是群聊，无法执行禁言操作。"
            
        if self.is_admin(user_id) or self.is_whitelisted(user_id):
            return "检测到辱骂行为，但对方是管理员/白名单用户，无法禁言。请用言语反击。"
            
        key = f"{group_id}_{user_id}"
        current_time = int(time.time())
        
        reset_days = self._get_cfg("mute_config", "reset_days", 1)
        
        if key in self.punish_times:
            last_time = self.punish_times[key]
            if (current_time - last_time) > (reset_days * 86400):
                self.punish_counts[key] = 0
                self.logger.info(f"群友 {user_id} 表现良好超过{reset_days}天，案底已重置")

        violation_count = self.punish_counts.get(key, 0) + 1
        self.punish_counts[key] = violation_count
        self.punish_times[key] = current_time 
            
        min_min = self._get_cfg("mute_config", "punish_min", 1)
        max_min = self._get_cfg("mute_config", "punish_max", 10)
        
        if min_min > max_min:
            min_min, max_min = max_min, min_min
        min_sec = min_min * 60
        max_sec = max_min * 60
        base_duration = random.randint(min_sec, max_sec)
        
        max_mul = self._get_cfg("mute_config", "max_multiplier", 10)
        if max_mul < 1:
            max_mul = 1
        
        multiplier = min(violation_count, max_mul) 
        final_duration = base_duration * multiplier
        
        MAX_MUTE_SECONDS = 30 * 24 * 3600
        if final_duration > MAX_MUTE_SECONDS:
            final_duration = MAX_MUTE_SECONDS
            
        success = await self.api_mute_member(int(group_id), int(user_id), final_duration)
        
        if success:
            self.enforce_mutes[key] = current_time + final_duration
            await self.save_data() 
            
            negative_pool = [13, 38, 46, 77, 178, 317, 323, 351, 355, 395]
            selected = random.sample(negative_pool, 2)
            await self._set_emoji_like(event, selected) 
            
            time_str = f"{final_duration // 60}分{final_duration % 60}秒" if final_duration % 60 != 0 else f"{final_duration // 60}分钟"
            msg = f"系统执行结果：已成功将用户 {user_id} 禁言 {time_str}。\n"
            
            if multiplier > 1:
                msg += f"这是用户第{violation_count}次违规，惩罚时长已根据规则自动翻倍x{multiplier}。\n"
                msg += "请在回复中嘲讽或严厉批评用户不知悔改，并明确告诉用户因为是第{violation_count}次违规，所以惩罚加重了！"
            else:
                msg += "这是初犯或未触发翻倍，请告知用户已被禁言以示惩戒，并用嘲讽的语气回复。"
            return msg
        else:
            return "尝试禁言失败，可能是Bot在群内权限不足（非管理员）。"

    # ==================== 生命周期与后台任务 ====================

    async def terminate(self):
        self.is_alive = False
        self._save_data_sync() 
        if self._bg_task:
            self._bg_task.cancel()
        self.logger.info("QQ群小管家 已停止")

    async def _background_loop(self):
        """主后台循环：包含宵禁检测和潜水清理"""
        client = None
        while not client:
            try:
                client = self._get_qq_client()
                if not client:
                    await asyncio.sleep(5)
            except Exception:
                await asyncio.sleep(5)
            
        try:
            if hasattr(client, "on_request"):
                client.on_request(self.on_group_request)
            if hasattr(client, "on_notice"):
                client.on_notice(self.on_group_notice)
        except Exception as e:
            self.logger.warning(f"注册事件失败: {e}")

        if self.last_check_time == 0:
            self.last_check_time = time.time()
            await self.save_data() 

        while self.is_alive:
            try:
                await self.update_member_cache()
                if not self.member_cache:
                    await asyncio.sleep(60)
                    continue

                await self.check_night_mode()

                raw_cfg = self._get_cfg("clean_config", "check_interval", 1)
                check_interval = raw_cfg * 3600 
                if check_interval < 300:
                    check_interval = 300 

                current_ts = time.time()
                if current_ts - self.last_check_time >= check_interval:
                    async with self.lock:
                        await self.check_inactive_members()
                    self.last_check_time = current_ts
                    await self.save_data() 
                    self.logger.info(f"潜水清理检查完成，下次将在 {check_interval} 秒后")

                await asyncio.sleep(60)

            except asyncio.CancelledError:
                break 
            except Exception as e:
                self.logger.error(f"后台循环异常: {e}")
                await asyncio.sleep(60)

    # ==================== 宵禁模式逻辑 ====================

    async def check_night_mode(self):
        if not self._get_bool_cfg("night_mode", "enable", False):
            return
        
        start_h = self._get_cfg("night_mode", "start_hour", 0)
        end_h = self._get_cfg("night_mode", "end_hour", 6)
        monitored = self._parse_list_config("clean_config", "monitored_groups")
        
        if not monitored:
            return

        now = datetime.now()
        current_h = now.hour
        current_m = now.minute

        if current_m != 0:
            return 

        client = self._get_qq_client()
        if not client:
            return

        action = None
        
        if current_h == start_h:
            action = True
            log_msg = "进入宵禁时间，开启全员禁言"
        elif current_h == end_h:
            action = False
            log_msg = "宵禁结束，解除全员禁言"
            
        if action is not None:
            self.logger.info(f"触发宵禁检查: {log_msg}")
            for gid in monitored:
                try:
                    await self.api_mute_whole(int(gid), action)
                    await asyncio.sleep(1) 
                except Exception as e:
                    self.logger.error(f"群 {gid} 宵禁操作失败: {e}")

    # ==================== 清理逻辑 ====================

    async def check_inactive_members(self):
        if not self.config.get("enabled", True) or not self.member_cache or not self.is_alive:
            return
        
        raw_inactive = self._get_cfg("clean_config", "inactive_days", 30)
        if raw_inactive < 1: 
            raw_inactive = 1 
        
        warning_days = self._get_cfg("clean_config", "warning_days", 7)
        if warning_days >= raw_inactive:
            warning_days = raw_inactive - 1
        if warning_days < 0:
            warning_days = 0

        auto_kick = self._get_bool_cfg("clean_config", "auto_kick", False)
        send_warning = self._get_bool_cfg("clean_config", "send_warning", False)
        
        current_ts = int(time.time())
        
        for gid, data in self.member_cache.items():
            members = data.get('members', [])
            kick_list = []    
            warning_list = []
            
            stats = {"total": len(members), "kick": 0, "warn": 0, "admin": 0, "active": 0}
            
            for m in members:
                uid = str(m.get('user_id'))
                if self.is_whitelisted(uid):
                    continue
                
                if m.get('role') in ['owner', 'admin'] and self._get_bool_cfg("clean_config", "skip_admins", True): 
                    stats["admin"] += 1
                    continue
                
                last = max(m.get('last_sent_time', 0) or m.get('join_time', 0), 
                           self.realtime_activity.get(str(gid), {}).get(uid, 0))
                
                if last == 0:
                    continue
                
                days_inactive = (current_ts - last) // 86400
                
                lv = self._parse_level(m.get('level', 0))
                if self._get_bool_cfg("clean_config", "enable_level_protection", True) and lv >= self._get_cfg("clean_config", "level_protection_threshold", 50): 
                    stats["active"] += 1
                    continue
                
                info = {'user_id': uid, 'days': days_inactive, 'remaining': raw_inactive - days_inactive}
                user_key = f"{gid}_{uid}"
                
                if days_inactive >= raw_inactive:
                    if not send_warning:
                         kick_list.append(info)
                    else:
                        warn_ts = self.warned_users.get(user_key, 0)
                        if warn_ts == 0: 
                            warning_list.append(info)
                        elif (current_ts - warn_ts) > 60: 
                            kick_list.append(info)
                        
                elif send_warning and days_inactive >= (raw_inactive - warning_days):
                    if user_key not in self.warned_users:
                        warning_list.append(info)
            
            stats["kick"] = len(kick_list)
            stats["warn"] = len(warning_list)
            stats["active"] = stats["total"] - stats["kick"] - stats["warn"] - stats["admin"]
            
            self.logger.info(f"群[{gid}] 扫描 (阈值{raw_inactive}天): 待踢[{stats['kick']}] 待警[{stats['warn']}]")

            if warning_list:
                await self.send_warnings(gid, warning_list)
            if kick_list:
                await self.handle_kick(gid, kick_list, auto_kick)
            if warning_list or kick_list:
                await asyncio.sleep(5)
        
        self._clean_cache_data()

    async def send_warnings(self, group_id: str, members: List[Dict]):
        if not self._get_bool_cfg("clean_config", "send_warning", False):
            return
        try:
            msg = "警告：以下成员长时间未发言\n请尽快冒泡，否则将进行清理\n\n"
            count = 0
            for m in members[:10]:
                msg += f"[CQ:at,qq={m['user_id']}] 未发言{m['days']}天\n"
                self.warned_users[f"{group_id}_{m['user_id']}"] = int(time.time())
                count += 1
            
            client = self._get_qq_client()
            if client: 
                await client.call_action('send_group_msg', group_id=int(group_id), message=msg)
                self.logger.info(f"已发送批量警告到群 {group_id} (包含{count}人)")
        except Exception as e:
            self.logger.error(f"发送警告失败: {e}")

    async def handle_kick(self, group_id: str, members: List[Dict], auto_kick: bool):
        client = self._get_qq_client()
        if not client: return
        
        if not auto_kick:
            msg = "建议清理名单:\n" + "\n".join(
                [f"{i+1}. {m['user_id']} ({m['days']}天)" for i, m in enumerate(members[:10])]
            )
            await client.call_action('send_group_msg', group_id=int(group_id), message=msg)
            return

        try:
            count = 0
            current_ts = int(time.time())
            for m in members:
                uid = str(m['user_id'])
                real_last = self.realtime_activity.get(str(group_id), {}).get(uid, 0)
                if (current_ts - real_last) < 60: 
                    self.logger.debug(f"拦截误杀: 群友 {uid} 刚刚冒泡了！")
                    self.warned_users.pop(f"{group_id}_{uid}", None)
                    continue

                if await self.api_kick_member(int(group_id), int(m['user_id'])):
                    count += 1
                    self.warned_users.pop(f"{group_id}_{uid}", None)
            
            if count > 0 and self._get_bool_cfg("clean_config", "send_kick_notification", True):
                 await client.call_action('send_group_msg', group_id=int(group_id), message=f"已自动清理 {count} 名不活跃成员")
        except Exception as e:
            self.logger.error(f"清理失败: {e}")

    def _clean_cache_data(self):
        try:
            inactive_limit = self._get_cfg("clean_config", "inactive_days", 30)
            expire_sec = (inactive_limit + 7) * 86400 
            current_ts = int(time.time())
            cleaned_count = 0
            
            for gid in list(self.realtime_activity.keys()):
                if gid not in self.member_cache:
                    continue

                current_member_ids = {str(m.get('user_id')) for m in self.member_cache[gid].get('members', [])}
                user_map = self.realtime_activity[gid]
                
                for uid in list(user_map.keys()):
                    should_delete = False
                    if uid not in current_member_ids:
                        should_delete = True
                    else:
                        ts = user_map[uid]
                        if ts > 0 and ts < current_ts and (current_ts - ts) > expire_sec:
                            should_delete = True
                    
                    if should_delete:
                        del user_map[uid]
                        cleaned_count += 1
                        
            if cleaned_count > 0:
                self.logger.debug(f"已清理 {cleaned_count} 条无效活跃度缓存")
                
        except Exception as e:
            self.logger.error(f"清理缓存数据失败: {e}")

    # ==================== 配置和数据并发保存逻辑 ====================

    def load_config_safe(self):
        try: 
            if self.config_path.exists():
                with open(self.config_path, 'r', encoding='utf-8-sig') as f: 
                    local_data = json.load(f)
                    if not isinstance(local_data, dict):
                        return
                    dynamic_keys = ['black_list', 'whitelist', 'monitored_groups', 'admin_list', 'black_groups']
                    for section, content in local_data.items():
                        if section not in self.config:
                            self.config[section] = {}
                        if isinstance(content, dict):
                            for k, v in content.items():
                                if k in dynamic_keys and k not in self.config[section]:
                                    self.config[section][k] = v
                                elif k not in self.config[section] and not self.config.get('enabled'):
                                    pass
        except Exception as e:
            self.logger.warning(f"读取本地配置出错: {e}")

    async def save_config(self):
        async with self.lock:
            self._save_config_sync()

    def _save_config_sync(self):
        try: 
            self.config_path.parent.mkdir(parents=True, exist_ok=True)
            with open(self.config_path, 'w', encoding='utf-8') as f: 
                json.dump(self.config, f, ensure_ascii=False, indent=2)
        except Exception as e:
            self.logger.error(f"保存配置出错: {e}")

    def load_data(self):
        if self.data_file.exists():
            try:
                with open(self.data_file, 'r', encoding='utf-8') as f:
                    d = json.load(f)
                    self.custom_welcome = d.get('custom_welcome', {})
                    self.punish_counts = d.get('punish_counts', {}) 
                    
                    raw_warned = d.get('warned_users', {})
                    self.warned_users = {}
                    for k, ts_val in raw_warned.items():
                        if isinstance(ts_val, str):
                            self.warned_users[k] = self._str_to_ts(ts_val)

                    raw_activity = d.get('realtime_activity', {})
                    self.realtime_activity = {}
                    for gid, users in raw_activity.items():
                        self.realtime_activity[gid] = {}
                        for uid, ts_val in users.items():
                            if isinstance(ts_val, str):
                                self.realtime_activity[gid][uid] = self._str_to_ts(ts_val)

                    raw_punish = d.get('punish_times', {})
                    self.punish_times = {}
                    for k, ts_val in raw_punish.items():
                        if isinstance(ts_val, str):
                            self.punish_times[k] = self._str_to_ts(ts_val)
                            
                    current_time = int(time.time())
                    raw_enforce = d.get('enforce_mutes', {})
                    self.enforce_mutes = {}
                    for k, val in raw_enforce.items():
                        ts = self._str_to_ts(val) if isinstance(val, str) else int(val)
                        if ts > current_time:  
                            self.enforce_mutes[k] = ts

                    raw_time = d.get('last_check_time', "")
                    self.last_check_time = self._str_to_ts(raw_time) if isinstance(raw_time, str) else 0

            except Exception as e: 
                self.logger.error(f"加载数据失败: {e}")

    async def save_data(self):
        async with self.lock:
            self._save_data_sync()

    def _save_data_sync(self):
        try:
            save_activity = {}
            for gid, users in self.realtime_activity.items():
                save_activity[gid] = {}
                for uid, ts in users.items():
                    save_activity[gid][uid] = self._ts_to_str(ts)
            
            save_punish = {}
            for k, ts in self.punish_times.items():
                save_punish[k] = self._ts_to_str(ts)
                
            save_warned = {}
            for k, ts in self.warned_users.items():
                save_warned[k] = self._ts_to_str(ts)

            current_time = int(time.time())
            keys_to_delete = []
            save_enforce = {}
            for k, ts in self.enforce_mutes.items():
                if current_time >= ts:
                    keys_to_delete.append(k)
                else:
                    save_enforce[k] = self._ts_to_str(ts)
            for k in keys_to_delete:
                del self.enforce_mutes[k]

            d = {
                'warned_users': save_warned, 
                'realtime_activity': save_activity,
                'custom_welcome': self.custom_welcome,
                'punish_counts': self.punish_counts,
                'punish_times': save_punish,
                'enforce_mutes': save_enforce,  
                'last_check_time': self._ts_to_str(self.last_check_time)
            }
            with open(self.data_file, 'w', encoding='utf-8') as f: 
                json.dump(d, f, ensure_ascii=False, indent=2)
        except Exception as e:
            self.logger.error(f"保存数据失败: {e}")

    def is_admin(self, user_id: str) -> bool:
        admins = self._parse_list_config("security_config", "admin_list")
        return int(user_id) in admins

    def is_whitelisted(self, user_id: str) -> bool:
        wl = self._parse_list_config("security_config", "whitelist")
        return int(user_id) in wl

    # ==================== 禁言辅助逻辑 ====================

    def _extract_target_id(self, event: AstrMessageEvent) -> Union[int, None]:
        chain = getattr(event.message_obj, 'message', [])
        for comp in chain:
            if getattr(comp, 'type', '').lower() == 'at':
                if hasattr(comp, 'data') and isinstance(comp.data, dict):
                    uid = comp.data.get('qq') or comp.data.get('user_id')
                    if uid:
                        return int(uid)
                if hasattr(comp, 'qq'):
                    return int(comp.qq)
                if hasattr(comp, 'user_id'):
                    return int(comp.user_id)
        for part in event.message_str.split():
            if part.isdigit() and len(part) >= 5:
                return int(part)
        return None

    def _extract_duration(self, event: AstrMessageEvent) -> int:
        args = event.message_str.split()
        default_min = self._get_cfg("mute_config", "default_duration", 10)
        target_id = self._extract_target_id(event)
        for arg in args:
            if arg.isdigit():
                val = int(arg)
                if target_id and val == target_id:
                    continue
                return val * 60
            if arg.lower().endswith('m') and arg[:-1].isdigit():
                return int(arg[:-1]) * 60
            if arg.lower().endswith('h') and arg[:-1].isdigit():
                return int(arg[:-1]) * 3600
            if arg.lower().endswith('s') and arg[:-1].isdigit():
                return int(arg[:-1])
        return default_min * 60

    async def _set_emoji_like(self, event: AstrMessageEvent, emoji_ids: Union[int, List[int]]):
        try:
            if isinstance(emoji_ids, int):
                emoji_ids = [emoji_ids]
            
            if not emoji_ids:
                return

            msg_id = None
            if hasattr(event.message_obj, 'message_id'):
                msg_id = int(event.message_obj.message_id)
            elif hasattr(event.message_obj, 'raw_message') and isinstance(event.message_obj.raw_message, dict):
                 msg_id = int(event.message_obj.raw_message.get('message_id', 0))
            
            if not msg_id:
                return

            client = getattr(event, 'bot', None)
            if not client: client = self._get_qq_client()
            if not client: return

            for eid in emoji_ids:
                if hasattr(client, 'set_msg_emoji_like'):
                    await client.set_msg_emoji_like(
                        message_id=msg_id,
                        emoji_id=eid,
                        emoji_type="1",
                        set=True
                    )
                else:
                    await client.call_action(
                        'set_msg_emoji_like',
                        message_id=msg_id,
                        emoji_id=eid,
                        emoji_type="1",
                        set=True
                    )
                if len(emoji_ids) > 1:
                    await asyncio.sleep(0.2)
                    
        except Exception as e:
            self.logger.warning(f"贴表情失败: {e}")

    async def api_mute_member(self, group_id: int, user_id: int, duration: int) -> bool:
        try:
            client = self._get_qq_client()
            if client:
                await client.call_action('set_group_ban', group_id=group_id, user_id=user_id, duration=duration)
                return True
        except Exception as e:
            self.logger.error(f"禁言失败: {e}")
        return False

    async def api_mute_whole(self, group_id: int, enable: bool = True) -> bool:
        try:
            client = self._get_qq_client()
            if client:
                await client.call_action('set_group_whole_ban', group_id=group_id, enable=enable)
                return True
        except Exception as e:
            self.logger.error(f"全员禁言失败: {e}")
        return False

    # ==================== 指令部分 ====================

    @filter.command("设置欢迎消息")
    async def cmd_set_welcome(self, event: AstrMessageEvent):
        if not self.is_admin(str(event.get_sender_id())):
            return
        group_id = getattr(event.message_obj, 'group_id', None)
        if not group_id:
            return
        content = event.message_str.split(maxsplit=1)[1].strip() if len(event.message_str.split()) > 1 else ""
        if not content:
            yield event.plain_result("请输入内容")
            return
        self.custom_welcome[str(group_id)] = content
        await self.save_data()
        yield event.plain_result(f"已设置")

    @filter.command("查看欢迎消息")
    async def cmd_get_welcome(self, event: AstrMessageEvent):
        if not self.is_admin(str(event.get_sender_id())):
            return
        group_id = getattr(event.message_obj, 'group_id', None)
        msg = self.custom_welcome.get(str(group_id)) or self._get_cfg("welcome_config", "welcome_msg")
        yield event.plain_result(f"当前欢迎：\n{msg}")

    @filter.command("管家帮助")
    async def cmd_help(self, event: AstrMessageEvent):
        if not self.is_admin(str(event.get_sender_id())):
            return
        help_text = (
            "QQ群小管家 使用帮助 \n"
            "【群管禁言】\n"
            "/禁言 [QQ/艾特] [时长(分)]\n"
            "/解禁 [QQ/艾特]\n"
            "/赦免 [QQ/艾特] (清除案底)\n"
            "/全员禁言 [开启/关闭]\n"
            "【核心开关】\n"
            "/管家监控 [列表/添加/删除] [群号]\n"
            "【名单管理】\n"
            "/白名单 [列表/添加/删除] [QQ号]\n"
            "/黑名单 [列表/添加/删除] [QQ号]\n"
            "【迎新设置】\n"
            "/设置欢迎消息 [内容]\n"
            "/查看欢迎消息\n"
            "【手动操作】\n"
            "/清理预览  /清理检查  /更新成员\n"
            "注意：仅插件管理员可用。"
        )
        yield event.plain_result(help_text)

    @filter.command("禁言")
    async def cmd_mute(self, event: AstrMessageEvent):
        if not self.is_admin(str(event.get_sender_id())):
            return
        group_id = getattr(event.message_obj, 'group_id', None)
        if not group_id:
            return
        
        target_id = self._extract_target_id(event)
        if not target_id:
            yield event.plain_result("未指定群友")
            return
            
        duration = self._extract_duration(event)
        
        if await self.api_mute_member(int(group_id), target_id, duration):
            positive_pool = [76, 271, 277, 299, 318, 319, 320, 337, 355, 356]
            selected = random.sample(positive_pool, 2) 
            await self._set_emoji_like(event, selected)
            
            client = self._get_qq_client()
            name = await self._get_member_name(int(group_id), target_id, client)
            
            mins = duration // 60
            yield event.plain_result(f"已禁言 {name}({target_id}) 共 {mins}分钟")
        else:
            yield event.plain_result("禁言失败")

    @filter.command("解禁")
    async def cmd_unmute(self, event: AstrMessageEvent):
        if not self.is_admin(str(event.get_sender_id())):
            return
        group_id = getattr(event.message_obj, 'group_id', None)
        if not group_id:
            return
        
        target_id = self._extract_target_id(event)
        if not target_id:
            yield event.plain_result("未指定群友")
            return
            
        key = f"{group_id}_{target_id}"
        if key in self.enforce_mutes:
            del self.enforce_mutes[key]
            await self.save_data()
            
        if await self.api_mute_member(int(group_id), target_id, 0):
            client = self._get_qq_client()
            name = await self._get_member_name(int(group_id), target_id, client)
            yield event.plain_result(f"已解禁 {name}({target_id})")
        else:
            yield event.plain_result("解禁失败")

    @filter.command("赦免")
    async def cmd_pardon(self, event: AstrMessageEvent):
        if not self.is_admin(str(event.get_sender_id())):
            return
        group_id = getattr(event.message_obj, 'group_id', None)
        if not group_id:
            return
        
        target_id = self._extract_target_id(event)
        if not target_id:
            yield event.plain_result("请指定要赦免的群友")
            return
            
        key = f"{group_id}_{target_id}"
        cleared = False
        
        if key in self.punish_counts:
            del self.punish_counts[key]
            cleared = True
            
        if key in self.punish_times:
            del self.punish_times[key]
            cleared = True
            
        if key in self.enforce_mutes:
            del self.enforce_mutes[key]
            cleared = True
            await self.api_mute_member(int(group_id), target_id, 0)
        
        client = self._get_qq_client()
        name = await self._get_member_name(int(group_id), target_id, client)

        if cleared:
            await self.save_data()
            yield event.plain_result(f"已赦免 {name}({target_id})，案底已清空。")
        else:
            yield event.plain_result(f"{name}({target_id}) 记录清白，无需赦免。")

    @filter.command("全员禁言")
    async def cmd_mute_all(self, event: AstrMessageEvent):
        if not self.is_admin(str(event.get_sender_id())):
            return
        group_id = getattr(event.message_obj, 'group_id', None)
        if not group_id:
            return
        
        msg = event.message_str
        enable = "关闭" not in msg and "解除" not in msg
        
        if await self.api_mute_whole(int(group_id), enable):
            state = "开启" if enable else "解除"
            yield event.plain_result(f"全员禁言已{state}")
        else:
            yield event.plain_result("操作失败 (权限不足?)")

    @filter.command("全员解禁")
    async def cmd_unmute_all(self, event: AstrMessageEvent):
        if not self.is_admin(str(event.get_sender_id())):
            return
        group_id = getattr(event.message_obj, 'group_id', None)
        if not group_id:
            return
        
        if await self.api_mute_whole(int(group_id), False):
            yield event.plain_result(f"全员禁言已解除")
        else:
            yield event.plain_result("操作失败")

    @filter.command("管家监控")
    async def cmd_monitor(self, event: AstrMessageEvent):
        if not self.is_admin(str(event.get_sender_id())): return
        async for res in self._handle_list_cmd(event, "clean_config", "monitored_groups", "管家监控群"): 
            yield res

    @filter.command("白名单")
    async def cmd_whitelist(self, event: AstrMessageEvent):
        if not self.is_admin(str(event.get_sender_id())): return
        async for res in self._handle_list_cmd(event, "security_config", "whitelist", "白名单"): 
            yield res

    @filter.command("黑名单")
    async def cmd_blacklist(self, event: AstrMessageEvent):
        if not self.is_admin(str(event.get_sender_id())): return
        async for res in self._handle_list_cmd(event, "security_config", "black_list", "黑名单"): 
            yield res

    async def _handle_list_cmd(self, event, section, key, name):
        msg = event.message_str.split()
        lst = self._parse_list_config(section, key)
        if len(msg) < 2 or msg[1] == "列表":
            yield event.plain_result(f"{name}: {lst}" if lst else f"{name}为空")
            return
        try:
            target = int(msg[2])
            if msg[1] == "添加":
                if target not in lst:
                    lst.append(target)
                    await self._set_cfg(section, key, lst)
                    if key == "black_list": self.blacklist = lst
                    yield event.plain_result(f"已添加 {target}")
                else:
                    yield event.plain_result("已存在")
            elif msg[1] == "删除":
                if target in lst:
                    lst.remove(target)
                    await self._set_cfg(section, key, lst)
                    if key == "black_list": self.blacklist = lst
                    yield event.plain_result(f"已删除 {target}")
                else:
                    yield event.plain_result(f"{target} 不在{name}中")
        except Exception as e:
            self.logger.warning(f"列表操作异常: {e}")
            yield event.plain_result("格式错误")

    @filter.command("清理检查")
    async def cmd_check(self, event: AstrMessageEvent):
        if not self.is_admin(str(event.get_sender_id())):
            return
        yield event.plain_result("开始检查...")
        if not self.member_cache:
            await self.update_member_cache()
        async with self.lock:
            await self.check_inactive_members()
        yield event.plain_result("完成")

    @filter.command("更新成员")
    async def cmd_update(self, event: AstrMessageEvent):
        if not self.is_admin(str(event.get_sender_id())):
            return
        yield event.plain_result("更新中...")
        await self.update_member_cache()
        yield event.plain_result("完成")

    @filter.command("清理预览")
    async def cmd_preview(self, event: AstrMessageEvent):
        if not self.is_admin(str(event.get_sender_id())):
            return
        group_id = getattr(event.message_obj, 'group_id', None)
        if not group_id:
            return
        
        data = self.member_cache.get(str(group_id))
        if not data: 
            yield event.plain_result("无数据")
            return
            
        raw_inactive = self._get_cfg("clean_config", "inactive_days", 30)
        if raw_inactive < 1:
            raw_inactive = 1 
        
        current = int(time.time())
        threshold = current - (raw_inactive * 86400)
        
        count = 0
        msg = f"📋 预览 (阈值 {raw_inactive} 天):\n"
        
        for m in data.get('members', []):
            if self.is_whitelisted(str(m.get('user_id'))):
                continue
            
            if self._get_bool_cfg("clean_config", "skip_admins", True) and m.get('role') in ['owner', 'admin']:
                continue
            
            last = max(m.get('last_sent_time', 0) or m.get('join_time', 0), 
                      self.realtime_activity.get(str(group_id), {}).get(str(m.get('user_id')), 0))
            
            if 0 < last < threshold:
                count += 1
                if count <= 10:
                    days = (current - last) // 86400
                    msg += f"{count}. {m.get('card') or m.get('nickname')} - {days}天\n"
        
        if count > 10:
            msg += f"...等 {count} 人"
        if count == 0:
            msg += "无需清理"
        yield event.plain_result(msg)

    def _get_qq_client(self):
        pm = self.context.platform_manager
        insts = pm.get_insts()
        if not insts:
            return None
        for inst in insts:
            p_name = str(getattr(inst, "platform_name", "")).lower()
            if "qq" in p_name or "onebot" in p_name:
                return inst.get_client()
        return insts[0].get_client()

    async def api_get_group_member_list(self, group_id: int) -> List[Dict]:
        try:
            client = self._get_qq_client()
            return await client.call_action('get_group_member_list', group_id=group_id) or [] if client else []
        except Exception:
            return []

    async def api_kick_member(self, group_id: int, user_id: int):
        try:
            client = self._get_qq_client()
            if client:
                await client.call_action('set_group_kick', group_id=group_id, user_id=user_id, reject_add_request=False)
                return True
        except Exception as e:
            self.logger.error(f"踢人请求失败: {e}")
        return False

    async def update_member_cache(self):
        async with self.lock:
            try:
                if not self.is_alive:
                    return
                monitored = self._parse_list_config("clean_config", "monitored_groups")
                if not monitored:
                    return
                self.logger.debug(f"更新成员缓存 ({len(monitored)}个群)")
                for gid in monitored:
                    mems = await self.api_get_group_member_list(int(gid))
                    if mems:
                        self.member_cache[str(gid)] = {'members': mems, 'update_time': int(time.time())}
                    await asyncio.sleep(0.5)
                self._save_data_sync() 
            except Exception as e:
                self.logger.error(f"更新失败: {e}")

    def _parse_level(self, level) -> int:
        try:
            return int(level)
        except Exception:
            return 0                