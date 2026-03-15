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
                msg += f"这是用户第{violation_count}次违规，惩罚时长已根据规则自动翻倍x{multiplier}。\n"
                msg += f"请在回复中嘲讽或严厉批评用户不知悔改，并明确告诉用户因为是第{violation_count}次违规，所以惩罚加重了！"
            else: 
                msg += "这是初犯或未触发翻倍，请告知用户已被禁言以示惩戒，并用嘲讽的语气回复。"
            return msg
        return "尝试禁言失败，可能是Bot在群内权限不足（非管理员）。"

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
        yield event.plain_result("已设置")

    @filter.command("查看欢迎消息")
    async def cmd_get_welcome(self, event: AstrMessageEvent):
        if not self.core.is_admin(str(event.get_sender_id())): return
        group_id = getattr(event.message_obj, 'group_id', None)
        msg = self.cfg.custom_welcome.get(str(group_id)) or self.cfg.get_cfg("welcome_config", "welcome_msg")
        yield event.plain_result(f"当前欢迎：\n{msg}")

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
