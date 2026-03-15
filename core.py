import asyncio
import time
import random
import os
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Union
from astrbot.api.event import AstrMessageEvent

class CoreLogic:
    def __init__(self, cfg_manager, context, logger):
        self.cfg = cfg_manager
        self.context = context
        self.logger = logger
        self.lock = asyncio.Lock()

    # ==================== 底层 QQ API ====================
    def get_qq_client(self):
        pm = self.context.platform_manager
        insts = pm.get_insts()
        if not insts: return None
        for inst in insts:
            if any(k in str(getattr(inst, "platform_name", "")).lower() for k in ["aiocqhttp"]): 
                return inst.get_client()
        return insts[0].get_client()

    async def api_get_group_member_list(self, group_id: int) -> List[Dict]:
        try:
            client = self.get_qq_client()
            return await client.call_action('get_group_member_list', group_id=group_id) or [] if client else []
        except Exception: 
            return []

    async def api_mute_member(self, group_id: int, user_id: int, duration: int) -> bool:
        try:
            client = self.get_qq_client()
            if client:
                await client.call_action('set_group_ban', group_id=group_id, user_id=user_id, duration=duration)
                return True
        except Exception as e: 
            self.logger.error(f"禁言失败: {e}")
        return False

    async def api_mute_whole(self, group_id: int, enable: bool = True) -> bool:
        try:
            client = self.get_qq_client()
            if client:
                await client.call_action('set_group_whole_ban', group_id=group_id, enable=enable)
                return True
        except Exception as e: 
            self.logger.error(f"全员禁言失败: {e}")
        return False

    async def api_kick_member(self, group_id: int, user_id: int):
        try:
            client = self.get_qq_client()
            if client:
                await client.call_action('set_group_kick', group_id=group_id, user_id=user_id, reject_add_request=False)
                return True
        except Exception: 
            pass
        return False

    async def set_emoji_like(self, event: AstrMessageEvent, emoji_ids: Union[int, List[int]]):
        try:
            if isinstance(emoji_ids, int): emoji_ids = [emoji_ids]
            if not emoji_ids: return

            msg_id = None
            if hasattr(event.message_obj, 'message_id'):
                msg_id = int(event.message_obj.message_id)
            elif hasattr(event.message_obj, 'raw_message') and isinstance(event.message_obj.raw_message, dict):
                 msg_id = int(event.message_obj.raw_message.get('message_id', 0))
            if not msg_id: return

            client = getattr(event, 'bot', None) or self.get_qq_client()
            if not client: return

            for eid in emoji_ids:
                if hasattr(client, 'set_msg_emoji_like'):
                    await client.set_msg_emoji_like(message_id=msg_id, emoji_id=eid, emoji_type="1", set=True)
                else:
                    await client.call_action('set_msg_emoji_like', message_id=msg_id, emoji_id=eid, emoji_type="1", set=True)
                if len(emoji_ids) > 1: await asyncio.sleep(0.2)
        except Exception as e: 
            self.logger.warning(f"贴表情失败: {e}")

    # ==================== 工具函数 ====================
    def is_admin(self, user_id: str) -> bool:
        return int(user_id) in self.cfg.parse_list("security_config", "admin_list")

    def is_whitelisted(self, user_id: str) -> bool:
        return int(user_id) in self.cfg.parse_list("security_config", "whitelist")

    def is_duplicate_event(self, key: str, ttl: int = 5) -> bool:
        current_time = time.time()
        self.cfg.event_dedup_cache = {k: v for k, v in self.cfg.event_dedup_cache.items() if current_time - v < ttl}
        if key in self.cfg.event_dedup_cache: return True
        self.cfg.event_dedup_cache[key] = current_time
        return False

    def extract_target_id(self, event: AstrMessageEvent) -> Union[int, None]:
        chain = getattr(event.message_obj, 'message', [])
        for comp in chain:
            if getattr(comp, 'type', '').lower() == 'at':
                if hasattr(comp, 'data') and isinstance(comp.data, dict):
                    uid = comp.data.get('qq') or comp.data.get('user_id')
                    if uid: return int(uid)
                if hasattr(comp, 'qq'): return int(comp.qq)
                if hasattr(comp, 'user_id'): return int(comp.user_id)
        for part in event.message_str.split():
            if part.isdigit() and len(part) >= 5: return int(part)
        return None

    def extract_duration(self, event: AstrMessageEvent) -> int:
        args = event.message_str.split()
        default_min = self.cfg.get_cfg("mute_config", "default_duration", 10)
        target_id = self.extract_target_id(event)
        for arg in args:
            if arg.isdigit():
                val = int(arg)
                if target_id and val == target_id: continue
                return val * 60
            if arg.lower().endswith('m') and arg[:-1].isdigit(): return int(arg[:-1]) * 60
            if arg.lower().endswith('h') and arg[:-1].isdigit(): return int(arg[:-1]) * 3600
            if arg.lower().endswith('s') and arg[:-1].isdigit(): return int(arg[:-1])
        return default_min * 60

    async def get_persona(self):
        try:
            if hasattr(self.context, "persona_manager"):
                pm = self.context.persona_manager
                if hasattr(pm, "personas") and isinstance(pm.personas, list) and len(pm.personas) > 0:
                    return getattr(pm.personas[0], "system_prompt", None) or getattr(pm.personas[0], "prompt", "")
            provider = self.context.get_using_provider()
            if provider and hasattr(provider, "system_prompt") and provider.system_prompt: 
                return provider.system_prompt
        except Exception: pass
        return (
            "你是一个高效、客观、偶尔带点毒舌的群管理员。你维护群组秩序，"
            "对违规者严厉，对友好者礼貌。说话简洁明了，不拖泥带水。"
        )

    async def get_member_name(self, group_id: int, user_id: int, client) -> str:
        try:
            if str(group_id) in self.cfg.member_cache:
                for m in self.cfg.member_cache[str(group_id)].get('members', []):
                    if int(m.get('user_id', 0)) == user_id: 
                        return m.get('card') or m.get('nickname') or str(user_id)
        except Exception: pass
        if client:
            try: return (await client.call_action('get_group_member_info', group_id=group_id, user_id=user_id, no_cache=False)).get('card', str(user_id))
            except Exception: pass
            try: return (await client.call_action('get_stranger_info', user_id=user_id, no_cache=False)).get('nickname', str(user_id))
            except Exception: pass
        return str(user_id)

    def get_img_from_config(self, config_key: str) -> str:
        file_list = self.cfg.config.get("welcome_config", {}).get(config_key, [])
        if not file_list or not isinstance(file_list, list): return ""
        valid_paths = []
        for path_str in file_list:
            if not path_str or not isinstance(path_str, str): continue
            p = Path(path_str)
            if not p.is_absolute():
                try: p = Path(self.cfg.data_dir) / path_str
                except Exception:
                    if not p.exists(): p = Path.cwd() / path_str
            if p.exists() and p.is_file(): valid_paths.append(str(p.absolute()))
        return random.choice(valid_paths) if valid_paths else ""

    def get_local_image_cq(self, path: str) -> str:
        if not path: return ""
        try:
            abs_path = os.path.abspath(path.replace("file:///", "").replace("file://", ""))
            if os.path.exists(abs_path): return f"[CQ:image,file=file:///{abs_path.replace(chr(92), '/')}]"
        except Exception: pass
        return ""

    # ==================== 事件处理业务 ====================
    async def on_group_request(self, event):
        if not self.cfg.config.get("enabled", True): return
        try:
            raw_gid, user_id = getattr(event, 'group_id', 0), int(getattr(event, 'user_id', 0))
            group_id = int(raw_gid)
            if group_id not in self.cfg.parse_list("clean_config", "monitored_groups"): return
            if self.is_duplicate_event(f"req_{group_id}_{user_id}", 5): return
            
            req_type, sub_type = getattr(event, 'request_type', None), getattr(event, 'sub_type', None)
            flag = getattr(event, 'flag', str(int(time.time()))) 
            
            if req_type == "group" and sub_type == "add":
                comment = getattr(event, 'comment', "") or "" 
                self.logger.debug(f"收到加群请求: 群[{group_id}] 用户[{user_id}] {comment}")
                client = self.get_qq_client()
                if not client: return

                if user_id in self.cfg.parse_list("security_config", "black_list"):
                    self.logger.debug(f"拒绝黑名单用户 {user_id}")
                    await client.call_action('set_group_add_request', flag=flag, sub_type=sub_type, approve=False, reason="主动退群、长期潜水以及黑名单不再同意进群，别申请啦！")
                    return

                if self.cfg.get_bool("approval_config", "auto_approve", False):
                    keywords = self.cfg.get_cfg("approval_config", "approval_keywords", [])
                    if keywords and any(str(k).lower() in comment.lower() for k in keywords if k): 
                        self.logger.debug(f"自动同意 {user_id} 入群")
                        await client.call_action('set_group_add_request', flag=flag, sub_type=sub_type, approve=True)
        except Exception as e: self.logger.error(f"处理入群请求出错: {e}")

    async def on_group_notice(self, event):
        if not self.cfg.config.get("enabled", True): return
        try: group_id = int(getattr(event, 'group_id', 0))
        except Exception: return

        notice_type, sub_type = getattr(event, 'notice_type', None), getattr(event, 'sub_type', None)
        user_id, client = int(getattr(event, 'user_id', 0)), self.get_qq_client()

        if notice_type == "group_ban":
            duration, operator_id = int(getattr(event, 'duration', 0)), int(getattr(event, 'operator_id', 0))
            self_id = int(getattr(client, 'self_id', 0)) if hasattr(client, 'self_id') else 0

            if (sub_type == "lift_ban" or duration == 0) and operator_id != self_id:
                user_key = f"{group_id}_{user_id}"
                current_time = int(time.time())
                if user_key in self.cfg.enforce_mutes:
                    expire_time = self.cfg.enforce_mutes[user_key]
                    if current_time < expire_time:
                        if client: await client.call_action('set_group_ban', group_id=group_id, user_id=user_id, duration=expire_time - current_time)
                        criminal_name = await self.get_member_name(group_id, user_id, client)
                        
                        llm_text = ""
                        try:
                            provider = self.context.get_using_provider()
                            if provider:
                                persona = await self.get_persona()
                                prompt = (
                                    f"【当前人设】\n{persona}\n\n"
                                    f"该群管理员提前解除因辱骂你的罪犯 {criminal_name} 的禁言。\n"
                                    f"你已经光速反制，把那个人重新关回去了。\n"
                                    f"严厉回复这名群管理员不要多管闲事。\n"
                                    f"这个罪犯 {criminal_name} 还没服完刑。\n"
                                    "要求：完全符合你的人设性格说话，真实自然。"
                                )
                                resp = await provider.text_chat(prompt, session_id=f"group_{group_id}")
                                llm_text = getattr(resp, 'completion_text', getattr(resp, 'text', str(resp)))
                        except Exception: pass
                        
                        msg = llm_text if llm_text else f"\n谁允许你擅自解除禁言了？\n“{criminal_name}”骂我的账还没算完！\n劝你不要多管闲事，哼~"
                        if client: await client.call_action('send_group_msg', group_id=group_id, message=f"[CQ:at,qq={operator_id}] \n{msg}")
                    else:
                        del self.cfg.enforce_mutes[user_key]
                        self.cfg.save_data()
            return 

        if group_id not in self.cfg.parse_list("clean_config", "monitored_groups"): return
        if group_id in self.cfg.parse_list("welcome_config", "black_groups"): return

        if notice_type == "group_increase":
            if self.is_duplicate_event(f"welcome_{group_id}_{user_id}", 5): return
            if not self.cfg.get_bool("welcome_config", "enable_welcome", False): return
            try:
                msg = self.cfg.custom_welcome.get(str(group_id)) or self.cfg.get_cfg("welcome_config", "welcome_msg", "欢迎！")
                chain = f"[CQ:at,qq={user_id}] " if self.cfg.get_bool("welcome_config", "is_at", True) else ""
                chain += msg.replace("{nickname}", str(user_id)).replace("{uid}", str(user_id))
                img = self.get_img_from_config("welcome_images_list")
                if img: chain += self.get_local_image_cq(img)
                if client:
                    await asyncio.sleep(1) 
                    await client.call_action('send_group_msg', group_id=group_id, message=chain)
            except Exception as e: self.logger.error(f"迎新失败: {e}")

        elif notice_type == "group_decrease" and sub_type == "leave":
            if self.is_duplicate_event(f"bye_{group_id}_{user_id}", 5): return
            if self.cfg.get_bool("security_config", "kick_black", False) and not self.is_whitelisted(str(user_id)):
                bl = self.cfg.parse_list("security_config", "black_list")
                if user_id not in bl:
                    bl.append(user_id)
                    self.cfg.set_cfg("security_config", "black_list", bl)
                    self.logger.debug(f"{user_id} 主动退群自动拉黑")
            if self.cfg.get_bool("welcome_config", "enable_bye", False):
                try:
                    nickname = str(user_id)
                    if client: nickname = (await client.call_action('get_stranger_info', user_id=user_id)).get('nickname', nickname)
                    msg = self.cfg.get_cfg("welcome_config", "bye_msg", "群友离开了。").format(username=nickname, userid=user_id)
                    img = self.get_img_from_config("bye_images_list")
                    if img: msg += self.get_local_image_cq(img)
                    if client: await client.call_action('send_group_msg', group_id=group_id, message=msg)
                except Exception: pass

    # ==================== 定时任务与清理 ====================
    async def background_loop(self):
        client = None
        while not client:
            client = self.get_qq_client()
            if not client: await asyncio.sleep(5)
            
        try:
            if hasattr(client, "on_request"): client.on_request(self.on_group_request)
            if hasattr(client, "on_notice"): client.on_notice(self.on_group_notice)
        except Exception as e: self.logger.warning(f"注册事件失败: {e}")

        if self.cfg.last_check_time == 0:
            self.cfg.last_check_time = time.time()
            self.cfg.save_data() 

        while self.cfg.is_alive:
            try:
                await self.update_member_cache()
                if not self.cfg.member_cache:
                    await asyncio.sleep(60)
                    continue

                await self.check_night_mode()

                interval = max(self.cfg.get_cfg("clean_config", "check_interval", 1) * 3600, 300)
                if time.time() - self.cfg.last_check_time >= interval:
                    async with self.lock: await self.check_inactive_members()
                    self.cfg.last_check_time = time.time()
                    self.cfg.save_data() 
                    self.logger.debug(f"潜水清理检查完成，下次将在 {interval} 秒后")

                await asyncio.sleep(60)
            except asyncio.CancelledError: break 
            except Exception as e:
                self.logger.error(f"后台循环异常: {e}")
                await asyncio.sleep(60)

    async def update_member_cache(self):
        if self.lock.locked(): return
        async with self.lock:
            try:
                if not self.cfg.is_alive: return
                monitored = self.cfg.parse_list("clean_config", "monitored_groups")
                if not monitored: return
                self.logger.debug(f"更新成员缓存 ({len(monitored)}个群)")
                for gid in monitored:
                    mems = await self.api_get_group_member_list(int(gid))
                    if mems: self.cfg.member_cache[str(gid)] = {'members': mems, 'update_time': int(time.time())}
                    await asyncio.sleep(0.5)
                self.cfg.save_data()
            except Exception as e: self.logger.error(f"更新失败: {e}")

    async def check_night_mode(self):
        if not self.cfg.get_bool("night_mode", "enable", False): return
        start_h, end_h = self.cfg.get_cfg("night_mode", "start_hour", 0), self.cfg.get_cfg("night_mode", "end_hour", 6)
        monitored = self.cfg.parse_list("clean_config", "monitored_groups")
        if not monitored: return

        now = datetime.now()
        if now.minute != 0: return 

        action, log_msg = None, ""
        if now.hour == start_h: action, log_msg = True, "进入宵禁时间，开启全员禁言"
        elif now.hour == end_h: action, log_msg = False, "宵禁结束，解除全员禁言"
            
        if action is not None:
            self.logger.debug(f"触发宵禁检查: {log_msg}")
            for gid in monitored:
                try:
                    await self.api_mute_whole(int(gid), action)
                    await asyncio.sleep(1) 
                except Exception as e:
                    self.logger.error(f"群 {gid} 宵禁操作失败: {e}")

    async def check_inactive_members(self):
        if not self.cfg.config.get("enabled", True) or not self.cfg.member_cache or not self.cfg.is_alive: return
        
        limit = max(self.cfg.get_cfg("clean_config", "inactive_days", 30), 1)
        warning_days = max(min(self.cfg.get_cfg("clean_config", "warning_days", 7), limit - 1), 0)
        auto_kick = self.cfg.get_bool("clean_config", "auto_kick", False)
        send_warning = self.cfg.get_bool("clean_config", "send_warning", False)
        current_ts = int(time.time())
        
        for gid, data in self.cfg.member_cache.items():
            members, kick_list, warning_list = data.get('members', []), [], []
            stats = {"total": len(members), "kick": 0, "warn": 0, "admin": 0, "active": 0}
            
            for m in members:
                uid = str(m.get('user_id'))
                if self.is_whitelisted(uid): continue
                if m.get('role') in ['owner', 'admin'] and self.cfg.get_bool("clean_config", "skip_admins", True): 
                    stats["admin"] += 1
                    continue
                
                last = max(m.get('last_sent_time', 0) or m.get('join_time', 0), self.cfg.realtime_activity.get(str(gid), {}).get(uid, 0))
                if last == 0: continue
                
                days_inactive = (current_ts - last) // 86400
                
                lv = int(m.get('level', 0))
                if self.cfg.get_bool("clean_config", "enable_level_protection", True) and lv >= self.cfg.get_cfg("clean_config", "level_protection_threshold", 50): 
                    stats["active"] += 1
                    continue
                
                info = {'user_id': uid, 'days': days_inactive}
                user_key = f"{gid}_{uid}"
                
                if days_inactive >= limit:
                    if not send_warning: kick_list.append(info)
                    else:
                        warn_ts = self.cfg.warned_users.get(user_key, 0)
                        if warn_ts == 0: warning_list.append(info)
                        elif (current_ts - warn_ts) > 60: kick_list.append(info)
                elif send_warning and days_inactive >= (limit - warning_days):
                    if user_key not in self.cfg.warned_users: warning_list.append(info)
                    
            stats["kick"] = len(kick_list)
            stats["warn"] = len(warning_list)
            stats["active"] = stats["total"] - stats["kick"] - stats["warn"] - stats["admin"]
            self.logger.debug(f"群[{gid}] 扫描 (阈值{limit}天): 待踢[{stats['kick']}] 待警[{stats['warn']}]")
            
            if warning_list: await self.send_warnings(gid, warning_list)
            if kick_list: await self.handle_kick(gid, kick_list, auto_kick)
            if warning_list or kick_list: await asyncio.sleep(5)
            
        self.clean_cache_data()

    async def send_warnings(self, group_id: str, members: List[Dict]):
        if not self.cfg.get_bool("clean_config", "send_warning", False): return
        try:
            msg = "警告：以下成员长时间未发言\n请尽快冒泡，否则将进行清理\n\n"
            count = 0
            for m in members[:10]:
                msg += f"[CQ:at,qq={m['user_id']}] 未发言{m['days']}天\n"
                self.cfg.warned_users[f"{group_id}_{m['user_id']}"] = int(time.time())
                count += 1
            client = self.get_qq_client()
            if client: 
                await client.call_action('send_group_msg', group_id=int(group_id), message=msg)
                self.logger.debug(f"已发送批量警告到群 {group_id} (包含{count}人)")
        except Exception as e: self.logger.error(f"发送警告失败: {e}")

    async def handle_kick(self, group_id: str, members: List[Dict], auto_kick: bool):
        client = self.get_qq_client()
        if not client: return
        if not auto_kick:
            msg = "建议清理名单:\n" + "\n".join([f"{i+1}. {m['user_id']} ({m['days']}天)" for i, m in enumerate(members[:10])])
            await client.call_action('send_group_msg', group_id=int(group_id), message=msg)
            return

        try:
            count = 0
            for m in members:
                uid = str(m['user_id'])
                if (int(time.time()) - self.cfg.realtime_activity.get(str(group_id), {}).get(uid, 0)) < 60:
                    self.logger.debug(f"拦截误杀: 群友 {uid} 刚刚冒泡了！")
                    self.cfg.warned_users.pop(f"{group_id}_{uid}", None)
                    continue

                if await self.api_kick_member(int(group_id), int(m['user_id'])):
                    count += 1
                    self.cfg.warned_users.pop(f"{group_id}_{uid}", None)
                    if self.cfg.get_bool("security_config", "cleaned_to_blacklist", False):
                        uid_int = int(m['user_id'])
                        bl = self.cfg.parse_list("security_config", "black_list")
                        if uid_int not in bl:
                            bl.append(uid_int)
                            self.cfg.set_cfg("security_config", "black_list", bl)
                            self.logger.debug(f"被清理成员 {uid_int} 已自动加入黑名单")
            
            if count > 0 and self.cfg.get_bool("clean_config", "send_kick_notification", True):
                 await client.call_action('send_group_msg', group_id=int(group_id), message=f"已自动清理 {count} 名不活跃成员")
        except Exception as e: self.logger.error(f"清理失败: {e}")

    def clean_cache_data(self):
        try:
            expire_sec = (self.cfg.get_cfg("clean_config", "inactive_days", 30) + 7) * 86400 
            current_ts = int(time.time())
            cleaned_count = 0
            
            for gid in list(self.cfg.realtime_activity.keys()):
                if gid not in self.cfg.member_cache: continue
                current_ids = {str(m.get('user_id')) for m in self.cfg.member_cache[gid].get('members', [])}
                user_map = self.cfg.realtime_activity[gid]
                
                for uid in list(user_map.keys()):
                    ts = user_map[uid]
                    should_delete = False
                    if uid not in current_ids: should_delete = True
                    elif 0 < ts < current_ts and (current_ts - ts) > expire_sec: should_delete = True
                    
                    if should_delete:
                        del user_map[uid]
                        cleaned_count += 1
                        
            if cleaned_count > 0:
                self.logger.debug(f"已清理 {cleaned_count} 条无效活跃度缓存")
        except Exception as e: self.logger.error(f"清理缓存数据失败: {e}")
        