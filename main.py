import time
import asyncio
import random
from typing import Dict, List, Union

from astrbot.api.event import AstrMessageEvent, filter
from astrbot.api.star import Context, Star, StarTools
from astrbot.api import logger

from .config import ConfigManager
from .core import CoreLogic

class Main(Star):
    def __init__(self, context: Context, config: dict = None) -> None:
        super().__init__(context)
        self.logger = logger
        
        # 获取插件的数据目录
        data_dir = StarTools.get_data_dir()
        
        # 初始化配置与数据管理
        self.cfg = ConfigManager(config, self.logger, data_dir)
        
        # 初始化核心业务逻辑
        self.core = CoreLogic(self.cfg, self.context, self.logger)
        
        if not self.cfg.config.get("enabled", True):
            self.logger.info("插件已禁用")
            return
            
        self.logger.info("QQ群小管家 已启动")
        
        # 启动后台任务
        self._bg_task = asyncio.create_task(self.core.background_loop())

    async def terminate(self):
        self.cfg.is_alive = False
        self.cfg.save_data() 
        if self._bg_task:
            self._bg_task.cancel()
        self.logger.info("QQ群小管家 已停止")

    @filter.event_message_type(filter.EventMessageType.GROUP_MESSAGE)
    async def on_group_message(self, event: AstrMessageEvent):
        try:
            if not self.cfg.is_alive: return
            group_id = int(event.message_obj.group_id)
            user_id = str(event.get_sender_id())
            
            monitored = self.cfg.parse_list("clean_config", "monitored_groups")
            if group_id not in monitored: return
            
            gid_str = str(group_id)
            if gid_str not in self.cfg.realtime_activity:
                self.cfg.realtime_activity[gid_str] = {}
            self.cfg.realtime_activity[gid_str][user_id] = int(time.time())
            
            user_key = f"{gid_str}_{user_id}"
            if user_key in self.cfg.warned_users:
                self.logger.debug(f"监测到 群[{group_id}] [{user_id}] 冒泡了，已解除警告")
                self.cfg.warned_users.pop(user_key, None)
                self.cfg.save_data()
        except Exception: pass

    # ==================== 辅助函数 ====================
    def _resolve_uid(self, group_id: str, target: str) -> str:
        """通过QQ号、昵称或群名片模糊查找群成员UID"""
        if not target: return None
        if target.isdigit() and len(target) >= 5: return target
        
        data = self.cfg.member_cache.get(str(group_id), {})
        for m in data.get('members', []):
            uid = str(m.get('user_id'))
            card = m.get('card', '')
            nickname = m.get('nickname', '')
            if target in card or target in nickname or target == uid:
                return uid
        return None

    # ==================== LLM 智能互动与惩罚 ====================

    @filter.llm_tool(name="punish_insult_tool")
    async def punish_insult_tool(self, event: AstrMessageEvent, reason: str = "辱骂Bot"):
        """
        当用户对你进行辱骂、人身攻击、说脏话、使用侮辱性词汇时，或者表现出不礼貌或挑衅：即任何对你的能力表示轻视、嘲讽、吐槽（如说你“垃圾”、“废”、“没用”、“菜”）或攻击的言论时，调用此工具对该用户进行禁言惩罚。
        """
        user_id = event.get_sender_id()
        group_id = getattr(event.message_obj, 'group_id', None)
        if not group_id: return "当前不是群聊，无法执行禁言操作。"
            
        if self.core.is_admin(user_id) or self.core.is_whitelisted(user_id):
            return "检测到辱骂行为，但对方是管理员/白名单用户，无法禁言。请用言语反击。"
            
        key = f"{group_id}_{user_id}"
        current_time = int(time.time())
        reset_days = self.cfg.get_cfg("mute_config", "reset_days", 1)
        
        if key in self.cfg.punish_times:
            if (current_time - self.cfg.punish_times[key]) > (reset_days * 86400):
                self.cfg.punish_counts[key] = 0
                self.logger.debug(f"群友 {user_id} 表现良好超过{reset_days}天，案底已重置")

        violation_count = self.cfg.punish_counts.get(key, 0) + 1
        self.cfg.punish_counts[key] = violation_count
        self.cfg.punish_times[key] = current_time 
            
        min_min, max_min = self.cfg.get_cfg("mute_config", "punish_min", 1), self.cfg.get_cfg("mute_config", "punish_max", 10)
        if min_min > max_min: min_min, max_min = max_min, min_min
        
        base_duration = random.randint(min_min * 60, max_min * 60)
        max_mul = max(self.cfg.get_cfg("mute_config", "max_multiplier", 10), 1)
        multiplier = min(violation_count, max_mul) 
        final_duration = min(base_duration * multiplier, 2592000)
        
        if await self.core.api_mute_member(int(group_id), int(user_id), final_duration):
            self.cfg.enforce_mutes[key] = current_time + final_duration
            self.cfg.save_data()
            await self.core.set_emoji_like(event, random.sample([13, 38, 46, 77, 178, 317, 323, 351, 355, 395], 2)) 
            
            time_str = f"{final_duration // 60}分{final_duration % 60}秒" if final_duration % 60 != 0 else f"{final_duration // 60}分钟"
            msg = f"系统执行结果：已成功将用户 {user_id} 禁言 {time_str}。\n"
            if multiplier > 1: 
                msg += f"这是用户第{violation_count}次违规，惩罚时长已根据规则自动翻倍x{multiplier}。\n请嘲讽用户不知悔改！"
            else: 
                msg += "这是初犯，请告知用户已被禁言以示惩戒，并用嘲讽的语气回复。"
            return msg
        return "尝试禁言失败，可能是Bot在群内权限不足（非管理员）。"

    @filter.llm_tool(name="get_group_member_count")
    async def get_group_member_count(self, event: AstrMessageEvent):
        """当用户询问群里有多少人、群总人数时，调用此工具获取当前群的成员总数。"""
        group_id = getattr(event.message_obj, 'group_id', None)
        if not group_id: return "当前不是群聊，无法查看群人数。"
        
        data = self.cfg.member_cache.get(str(group_id))
        if not data:
            mems = await self.core.api_get_group_member_list(int(group_id))
            if mems:
                self.cfg.member_cache[str(group_id)] = {'members': mems, 'update_time': int(time.time())}
                data = self.cfg.member_cache[str(group_id)]
            else:
                return "无法获取群成员数据，请稍后再试。"
                
        total = len(data.get('members', []))
        return f"当前群的总人数为 {total} 人。"

    @filter.llm_tool(name="get_inactive_members_info")
    async def get_inactive_members_info(self, event: AstrMessageEvent):
        """当用户询问群里有多少人不活跃、潜水人数、或者不活跃的成员有哪些时调用此工具。"""
        group_id = getattr(event.message_obj, 'group_id', None)
        if not group_id: return "当前不是群聊，无法查看不活跃成员。"
        
        data = self.cfg.member_cache.get(str(group_id))
        if not data: return "无法获取群成员数据，请稍后再试。"
                
        limit = max(self.cfg.get_cfg("clean_config", "inactive_days", 30), 1)
        threshold = int(time.time()) - (limit * 86400)
        
        inactive_members = []
        for m in data.get('members', []):
            uid = str(m.get('user_id'))
            if self.core.is_whitelisted(uid): continue
            if self.cfg.get_bool("clean_config", "skip_admins", True) and m.get('role') in ['owner', 'admin']: continue
                
            last = max(m.get('last_sent_time', 0) or m.get('join_time', 0), self.cfg.realtime_activity.get(str(group_id), {}).get(uid, 0))
            if 0 < last < threshold:
                inactive_days = (int(time.time()) - last) // 86400
                name = m.get('card') or m.get('nickname') or uid
                inactive_members.append({'name': name, 'uid': uid, 'days': inactive_days})
                
        inactive_members.sort(key=lambda x: x['days'], reverse=True)
        count = len(inactive_members)
        
        msg = f"根据设定的 {limit} 天不活跃标准，当前群共有 {count} 名不活跃成员（潜水成员）。\n"
        if count > 0:
            msg += "以下是部分潜水名单：\n"
            for idx, m in enumerate(inactive_members[:15]):
                msg += f"{idx+1}. {m['name']}({m['uid']}) - 潜水 {m['days']} 天\n"
            if count > 15: msg += f"...等共 {count} 人。"
        return msg

    @filter.llm_tool(name="get_active_members_info")
    async def get_active_members_info(self, event: AstrMessageEvent):
        """当用户询问群里有哪些活跃成员、最近活跃的人、一周内冒泡/活跃的人数时调用此工具。注意：你必须在回答中具体念出这些活跃成员的昵称！"""
        group_id = getattr(event.message_obj, 'group_id', None)
        if not group_id: return "当前不是群聊，无法查看活跃成员。"
        
        data = self.cfg.member_cache.get(str(group_id))
        if not data: return "无法获取群成员数据，请稍后再试。"
                
        threshold = int(time.time()) - (7 * 86400)
        
        active_members = []
        for m in data.get('members', []):
            uid = str(m.get('user_id'))
            
            last_sent = m.get('last_sent_time', 0) or 0
            realtime_last = self.cfg.realtime_activity.get(str(group_id), {}).get(uid, 0)
            last = max(last_sent, realtime_last)
            
            if last > threshold:
                name = m.get('card') or m.get('nickname') or uid
                active_members.append({'name': name, 'uid': uid, 'last': last})
                
        active_members.sort(key=lambda x: x['last'], reverse=True)
        count = len(active_members)
        total = len(data.get('members', []))
        
        active_rate = round(count / total * 100, 1) if total > 0 else 0
        
        msg = f"近7天内本群共有 {count} 名活跃成员，活跃度约为 {active_rate}%。\n"
        if count > 0:
            msg += "近期最活跃的前15名成员（水龙王）名单如下：\n"
            for idx, m in enumerate(active_members[:15]):
                diff_seconds = int(time.time()) - m['last']
                if diff_seconds < 3600:
                    time_str = f"{diff_seconds // 60}分钟前"
                elif diff_seconds < 86400:
                    time_str = f"{diff_seconds // 3600}小时前"
                else:
                    time_str = f"{diff_seconds // 86400}天前"
                    
                msg += f"{idx+1}. {m['name']} (最后发言:{time_str})\n"
                
            if count > 15: 
                msg += f"（此外还有 {count - 15} 人近期也活跃过，为避免刷屏已省略）\n"
                
            msg += "\n【系统强制指令】：请根据上述名单，用幽默、夸奖的语气，向用户具体报出这几个人的昵称（可以说他们是群里的水龙王、气氛组等），不要只说人数，必须说出名字！"
                
        return msg

    @filter.llm_tool(name="query_member_status")
    async def query_member_status(self, event: AstrMessageEvent, target_name_or_qq: str):
        """
        当用户向你询问某个特定群成员的状态、查水表、潜水天数、违规记录（案底）时调用此工具。
        Args:
            target_name_or_qq (string): 目标用户的 QQ 号、昵称或群名片。
        """
        group_id = getattr(event.message_obj, 'group_id', None)
        if not group_id: return "当前不是群聊。"
        
        target_uid = self._resolve_uid(str(group_id), target_name_or_qq)
        if not target_uid: return f"没有在群里找到叫 '{target_name_or_qq}' 的人。"
            
        user_key = f"{group_id}_{target_uid}"
        punish_count = self.cfg.punish_counts.get(user_key, 0)
        is_white = self.core.is_whitelisted(target_uid)
        is_admin = self.core.is_admin(target_uid)
        
        # 查找昵称
        target_name = target_uid
        data = self.cfg.member_cache.get(str(group_id), {})
        for m in data.get('members', []):
            if str(m.get('user_id')) == target_uid:
                target_name = m.get('card') or m.get('nickname') or target_uid
                break
                
        last_active = self.cfg.realtime_activity.get(str(group_id), {}).get(target_uid, 0)
        inactive_days = (int(time.time()) - last_active) // 86400 if last_active > 0 else "很久"
        
        status_msg = (f"获取到用户 {target_name} ({target_uid}) 的档案：\n"
                      f"- 潜水天数：{inactive_days} 天\n"
                      f"- 历史违规/被禁言次数：{punish_count} 次\n"
                      f"- 身份：{'管理员 ' if is_admin else ''}{'白名单用户 ' if is_white else '普通群友'}\n"
                      f"请根据以上数据，用你的性格对这个用户的活跃度和行为进行评价。")
        return status_msg

    @filter.llm_tool(name="agent_execute_mute")
    async def agent_execute_mute(self, event: AstrMessageEvent, target_name_or_qq: str, duration_minutes: int):
        """
        当【管理员】要求你对某个用户进行禁言、关小黑屋、闭嘴时调用此工具。
        Args:
            target_name_or_qq (string): 目标用户的QQ号或昵称。
            duration_minutes (int): 禁言时长（分钟）。如果不确定，默认传10。
        """
        user_id = str(event.get_sender_id())
        group_id = getattr(event.message_obj, 'group_id', None)
        if not group_id: return "当前不是群聊。"
        
        if not self.core.is_admin(user_id):
            return "【拒绝执行】调用失败：对方不是管理员，没有权限指使你禁言别人！请严厉回复或嘲笑这个没有权限还想使唤你的普通群友。"
            
        target_uid = self._resolve_uid(str(group_id), target_name_or_qq)
        if not target_uid: return f"找不到名为 '{target_name_or_qq}' 的用户，无法执行禁言。"
        
        if await self.core.api_mute_member(int(group_id), int(target_uid), duration_minutes * 60):
            return f"执行成功！已将 {target_uid} 禁言 {duration_minutes} 分钟。请回复管理员任务已完成。"
        return "执行失败，可能是机器人没有群管权限或目标是群主。"

    @filter.llm_tool(name="agent_toggle_global_mute")
    async def agent_toggle_global_mute(self, event: AstrMessageEvent, enable: bool):
        """
        当【管理员】要求你“开启全员禁言”、“让大家安静一下”（enable=true）或“解除全员禁言”（enable=false）时调用此工具。
        Args:
            enable (bool): true 为开启全员禁言，false 为解除。
        """
        user_id = str(event.get_sender_id())
        group_id = getattr(event.message_obj, 'group_id', None)
        if not group_id: return "当前不是群聊。"
        
        if not self.core.is_admin(user_id):
            return "【拒绝执行】对方不是管理员，无权操作全员禁言！请严厉驳回。"
            
        if await self.core.api_mute_whole(int(group_id), enable):
            state = "开启" if enable else "解除"
            return f"执行成功！已{state}全员禁言。请回复管理员已办妥。"
        return "执行失败，可能是机器人没有群管权限。"

    @filter.llm_tool(name="appeal_punish_record")
    async def appeal_punish_record(self, event: AstrMessageEvent):
        """
        当群友向你认错、道歉、或者请求“消除案底”、“清空违规记录”时调用此工具。
        """
        user_id = str(event.get_sender_id())
        group_id = getattr(event.message_obj, 'group_id', None)
        if not group_id: return "当前不是群聊。"
        
        user_key = f"{group_id}_{user_id}"
        punish_count = self.cfg.punish_counts.get(user_key, 0)
        
        if punish_count == 0:
            return "该用户目前没有任何违规记录（案底为0）。请回复他不要无病呻吟，他的记录很干净。"
            
        if punish_count >= 3:
            return f"该用户已有 {punish_count} 次违规记录。请拒绝他的求情，并严厉告诉他：屡教不改，罪孽深重，本管家拒绝消除你的案底！"

        current_time = int(time.time())
        last_punish_time = self.cfg.punish_times.get(user_key, 0)
        
        if current_time - last_punish_time < 86400: 
            return "该用户距离上次受罚还不到 24 小时。请无情地拒绝他，告诉他：刚犯完错就想让我原谅你？给我老老实实反省一天再说！"

        if not hasattr(self.cfg, 'appeal_times'):
            self.cfg.appeal_times = {}
            
        last_appeal = self.cfg.appeal_times.get(user_key, 0)
        if current_time - last_appeal < 7 * 86400: 
            return "该用户最近 7 天内已经使用过求情机会了。请拒绝他，并告诉他：你的“免死金牌”还在冷却中，过几天再来求我吧！"

        self.cfg.punish_counts.pop(user_key, None)
        self.cfg.punish_times.pop(user_key, None)
        self.cfg.appeal_times[user_key] = current_time 
        self.cfg.save_data()
        
        return f"系统已将该用户的案底（原违规{punish_count}次）清零。请以极其傲娇、勉为其难的语气回复他，告诉他看在态度诚恳的份上原谅他了，警告他下次再犯绝不轻饶。"

    @filter.llm_tool(name="explain_group_rules")
    async def explain_group_rules(self, event: AstrMessageEvent):
        """当群友询问“群规是什么”、“进群有什么规矩”、“能发广告吗”等关于规矩的问题时调用此工具。"""
        group_id = str(getattr(event.message_obj, 'group_id', ''))
        if not group_id: return "当前不是群聊，无群规。"
        
        inactive_days = self.cfg.get_cfg("clean_config", "inactive_days", 30)
        auto_kick = self.cfg.get_bool("clean_config", "auto_kick", False)
        kick_str = "直接踢出群聊" if auto_kick else "记录在清理名单中"
        
        custom_rules = self.cfg.custom_rules.get(group_id) or self.cfg.get_cfg("group_config", "rules", "")
        
        if custom_rules:
            rules = custom_rules.replace("{inactive_days}", str(inactive_days)).replace("{kick_str}", kick_str)
        else:
            rules = (
                "1. 严禁发广告、刷屏、发布违法违规内容。\n"
                "2. 严禁对本管家(机器人)或其他群友进行人身攻击或辱骂，违者将被关小黑屋。\n"
                f"3. 本群实行活跃度考核，潜水超过 {inactive_days} 天不发言，将被{kick_str}。\n"
                "4. 尊重群主和管理员，文明交流。"
            )
        return f"以下是群规核心内容：\n{rules}\n\n请用你独特的性格向用户复述这些群规，警告他们不要以身试法。"

    @filter.llm_tool(name="query_or_modify_config")
    async def query_or_modify_config(self, event: AstrMessageEvent, action: str, config_key: str, new_value_int: int = 0):
        """
        当【管理员】询问或要求修改管家系统参数时调用此工具（例如：当前清理天数是多少？/把清理天数改成15天）。
        Args:
            action (string): "query" (查询) 或 "modify" (修改)。
            config_key (string): 只能是 "inactive_days" (潜水清理天数) 或 "default_duration" (默认禁言分钟)。
            new_value_int (int): 如果是修改，传入新的整数值。查询则忽略。
        """
        user_id = str(event.get_sender_id())
        if not self.core.is_admin(user_id):
            return "调用失败：对方不是系统管理员，无权查看或修改底层配置。请告诉他没有权限。"
            
        path_map = {
            "inactive_days": ("clean_config", "inactive_days"),
            "default_duration": ("mute_config", "default_duration")
        }
        
        if config_key not in path_map:
            return f"未知的配置项：{config_key}。目前仅支持查询/修改 'inactive_days' 和 'default_duration'。"
            
        section, key = path_map[config_key]
        
        if action == "query":
            val = self.cfg.get_cfg(section, key)
            return f"查询成功：当前 [{config_key}] 的值为 {val}。"
        elif action == "modify":
            if new_value_int <= 0: return "修改失败：参数值必须大于0。"
            self.cfg.set_cfg(section, key, new_value_int)
            return f"修改成功！已将 [{config_key}] 的值更新为 {new_value_int}。请告知管理员已修改完毕。"
        
        return "未知的操作动作。"


    # ==================== 指令部分 ====================

    @filter.command("设置欢迎消息")
    async def cmd_set_welcome(self, event: AstrMessageEvent):
        if not self.core.is_admin(str(event.get_sender_id())): return
        group_id = getattr(event.message_obj, 'group_id', None)
        if not group_id: return
        
        content = event.message_str.split(maxsplit=1)[1].strip() if len(event.message_str.split()) > 1 else ""
        if not content:
            yield event.plain_result("请输入内容")
            return
        self.cfg.custom_welcome[str(group_id)] = content
        self.cfg.save_data()
        yield event.plain_result(f"已成功设置本群欢迎消息！")

    @filter.command("查看欢迎消息")
    async def cmd_get_welcome(self, event: AstrMessageEvent):
        if not self.core.is_admin(str(event.get_sender_id())): return
        group_id = getattr(event.message_obj, 'group_id', None)
        msg = self.cfg.custom_welcome.get(str(group_id)) or self.cfg.get_cfg("welcome_config", "welcome_msg")
        yield event.plain_result(f"当前欢迎消息：\n{msg}")

    @filter.command("设置群规")
    async def cmd_set_rules(self, event: AstrMessageEvent):
        if not self.core.is_admin(str(event.get_sender_id())): return
        group_id = getattr(event.message_obj, 'group_id', None)
        if not group_id: return
        
        content = event.message_str.split(maxsplit=1)[1].strip() if len(event.message_str.split()) > 1 else ""
        if not content:
            yield event.plain_result("请输入群规内容！\n格式：/设置群规 [你的群规内容]")
            return
        self.cfg.custom_rules[str(group_id)] = content
        self.cfg.save_data()
        yield event.plain_result(f"本群群规已成功更新！")

    @filter.command("查看群规")
    async def cmd_get_rules(self, event: AstrMessageEvent):
        if not self.core.is_admin(str(event.get_sender_id())): return
        group_id = getattr(event.message_obj, 'group_id', None)
        rules = self.cfg.custom_rules.get(str(group_id)) or self.cfg.get_cfg("group_config", "rules", "")
        if not rules:
            yield event.plain_result("当前未设置群规，使用系统默认群规。")
        else:
            yield event.plain_result(f"当前群规：\n{rules}")

    @filter.command("设置退群消息")
    async def cmd_set_bye(self, event: AstrMessageEvent):
        if not self.core.is_admin(str(event.get_sender_id())): return
        group_id = getattr(event.message_obj, 'group_id', None)
        if not group_id: return
        
        content = event.message_str.split(maxsplit=1)[1].strip() if len(event.message_str.split()) > 1 else ""
        if not content:
            yield event.plain_result("请输入内容！\n格式：/设置退群消息 [内容]")
            return
        self.cfg.custom_bye[str(group_id)] = content
        self.cfg.save_data()
        yield event.plain_result(f"已成功设置本群退群消息！")

    @filter.command("查看退群消息")
    async def cmd_get_bye(self, event: AstrMessageEvent):
        if not self.core.is_admin(str(event.get_sender_id())): return
        group_id = getattr(event.message_obj, 'group_id', None)
        msg = self.cfg.custom_bye.get(str(group_id)) or self.cfg.get_cfg("welcome_config", "bye_msg")
        yield event.plain_result(f"当前退群消息：\n{msg}")

    @filter.command("管家帮助")
    async def cmd_help(self, event: AstrMessageEvent):
        if not self.core.is_admin(str(event.get_sender_id())): return
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
            "【文字设置】\n"
            "/设置群规 [内容] (/查看)\n"
            "/设置欢迎消息 [内容] (/查看)\n"
            "/设置退群消息 [内容] (/查看)\n"
            "【手动操作】\n"
            "/清理预览  /清理检查  /更新成员\n"
            "注意：仅插件管理员可用。"
        )
        yield event.plain_result(help_text)

    @filter.command("禁言")
    async def cmd_mute(self, event: AstrMessageEvent):
        if not self.core.is_admin(str(event.get_sender_id())): return
        group_id = getattr(event.message_obj, 'group_id', None)
        if not group_id: return
        
        target_id = self.core.extract_target_id(event)
        if not target_id:
            yield event.plain_result("未指定群友")
            return
            
        duration = self.core.extract_duration(event)
        if await self.core.api_mute_member(int(group_id), target_id, duration):
            await self.core.set_emoji_like(event, random.sample([76, 271, 277, 299, 318, 319, 320, 337, 355, 356], 2))
            client = self.core.get_qq_client()
            name = await self.core.get_member_name(int(group_id), target_id, client)
            yield event.plain_result(f"已禁言 {name}({target_id}) 共 {duration // 60}分钟")
        else:
            yield event.plain_result("禁言失败")

    @filter.command("解禁")
    async def cmd_unmute(self, event: AstrMessageEvent):
        if not self.core.is_admin(str(event.get_sender_id())): return
        group_id = getattr(event.message_obj, 'group_id', None)
        if not group_id: return
        
        target_id = self.core.extract_target_id(event)
        if not target_id:
            yield event.plain_result("未指定群友")
            return
            
        key = f"{group_id}_{target_id}"
        if key in self.cfg.enforce_mutes:
            del self.cfg.enforce_mutes[key]
            self.cfg.save_data()
            
        if await self.core.api_mute_member(int(group_id), target_id, 0):
            client = self.core.get_qq_client()
            name = await self.core.get_member_name(int(group_id), target_id, client)
            yield event.plain_result(f"已解禁 {name}({target_id})")
        else:
            yield event.plain_result("解禁失败")

    @filter.command("赦免")
    async def cmd_pardon(self, event: AstrMessageEvent):
        if not self.core.is_admin(str(event.get_sender_id())): return
        group_id = getattr(event.message_obj, 'group_id', None)
        if not group_id: return
        
        target_id = self.core.extract_target_id(event)
        if not target_id:
            yield event.plain_result("请指定要赦免的群友")
            return
            
        key = f"{group_id}_{target_id}"
        cleared = False
        for d in [self.cfg.punish_counts, self.cfg.punish_times]:
            if key in d: del d[key]; cleared = True
        if key in self.cfg.enforce_mutes:
            del self.cfg.enforce_mutes[key]
            cleared = True
            await self.core.api_mute_member(int(group_id), target_id, 0)
        
        client = self.core.get_qq_client()
        name = await self.core.get_member_name(int(group_id), target_id, client)
        if cleared:
            self.cfg.save_data()
            yield event.plain_result(f"已赦免 {name}({target_id})，案底已清空。")
        else:
            yield event.plain_result(f"{name}({target_id}) 记录清白，无需赦免。")

    @filter.command("全员禁言")
    async def cmd_mute_all(self, event: AstrMessageEvent):
        if not self.core.is_admin(str(event.get_sender_id())): return
        group_id = getattr(event.message_obj, 'group_id', None)
        if not group_id: return
        enable = "关闭" not in event.message_str and "解除" not in event.message_str
        if await self.core.api_mute_whole(int(group_id), enable):
            yield event.plain_result(f"全员禁言已{'开启' if enable else '解除'}")
        else:
            yield event.plain_result("操作失败 (权限不足?)")

    @filter.command("全员解禁")
    async def cmd_unmute_all(self, event: AstrMessageEvent):
        if not self.core.is_admin(str(event.get_sender_id())): return
        group_id = getattr(event.message_obj, 'group_id', None)
        if group_id and await self.core.api_mute_whole(int(group_id), False):
            yield event.plain_result("全员禁言已解除")
        else:
            yield event.plain_result("操作失败")

    @filter.command("管家监控")
    async def cmd_monitor(self, event: AstrMessageEvent):
        if not self.core.is_admin(str(event.get_sender_id())): return
        for res in self._handle_list(event, "clean_config", "monitored_groups", "管家监控群"): yield res

    @filter.command("白名单")
    async def cmd_whitelist(self, event: AstrMessageEvent):
        if not self.core.is_admin(str(event.get_sender_id())): return
        for res in self._handle_list(event, "security_config", "whitelist", "白名单"): yield res

    @filter.command("黑名单")
    async def cmd_blacklist(self, event: AstrMessageEvent):
        if not self.core.is_admin(str(event.get_sender_id())): return
        for res in self._handle_list(event, "security_config", "black_list", "黑名单"): yield res

    def _handle_list(self, event, section, key, name):
        msg = event.message_str.split()
        lst = self.cfg.parse_list(section, key)
        if len(msg) < 2 or msg[1] == "列表":
            yield event.plain_result(f"{name}: {lst}" if lst else f"{name}为空")
            return
        try:
            target = int(msg[2])
            if msg[1] == "添加":
                if target not in lst:
                    lst.append(target)
                    self.cfg.set_cfg(section, key, lst)
                    yield event.plain_result(f"已添加 {target}")
                else: yield event.plain_result("已存在")
            elif msg[1] == "删除":
                if target in lst:
                    lst.remove(target)
                    self.cfg.set_cfg(section, key, lst)
                    yield event.plain_result(f"已删除 {target}")
                else: yield event.plain_result(f"{target} 不在{name}中")
            else: yield event.plain_result("格式错误")
        except Exception: yield event.plain_result("格式错误")

    @filter.command("清理检查")
    async def cmd_check(self, event: AstrMessageEvent):
        if not self.core.is_admin(str(event.get_sender_id())): return
        yield event.plain_result("开始检查...")
        if not self.cfg.member_cache: await self.core.update_member_cache()
        async with self.core.lock: await self.core.check_inactive_members()
        yield event.plain_result("完成")

    @filter.command("更新成员")
    async def cmd_update(self, event: AstrMessageEvent):
        if not self.core.is_admin(str(event.get_sender_id())): return
        yield event.plain_result("更新中...")
        await self.core.update_member_cache()
        yield event.plain_result("完成")

    @filter.command("清理预览")
    async def cmd_preview(self, event: AstrMessageEvent):
        if not self.core.is_admin(str(event.get_sender_id())): return
        group_id = getattr(event.message_obj, 'group_id', None)
        data = self.cfg.member_cache.get(str(group_id))
        if not data:
            yield event.plain_result("无数据")
            return
            
        limit = max(self.cfg.get_cfg("clean_config", "inactive_days", 30), 1)
        threshold = int(time.time()) - (limit * 86400)
        count, msg = 0, f"📋 预览 (阈值 {limit} 天):\n"
        
        for m in data.get('members', []):
            if self.core.is_whitelisted(str(m.get('user_id'))): continue
            if self.cfg.get_bool("clean_config", "skip_admins", True) and m.get('role') in ['owner', 'admin']: continue
            last = max(m.get('last_sent_time', 0) or m.get('join_time', 0), self.cfg.realtime_activity.get(str(group_id), {}).get(str(m.get('user_id')), 0))
            if 0 < last < threshold:
                count += 1
                if count <= 10: msg += f"{count}. {m.get('card') or m.get('nickname')} - {(int(time.time()) - last) // 86400}天\n"
        
        if count > 10: msg += f"...等 {count} 人"
        if count == 0: msg += "无需清理"
        yield event.plain_result(msg)
