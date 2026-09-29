# Changelog

All notable changes to this project will be documented in this file.

## Unreleased

### Fixed
- Direct messages now use the stable `senderId` for session routing while
  preserving the inbound `from` value for outbound delivery.

## [1.1.0] - 2026-05-17

### Added
- **增强媒体发送能力**：新增本地附件路径自动识别逻辑（支持图片、视频、音频及多种文档格式）。
- **音频/语音支持**：完善 `audioAsVoice` 处理，支持发送原生微信语音。
- **智能路径路由**：优化 `_resolve_send_chat_id`，支持在没有明确指定目标时自动路由至当前活跃微信聊天窗口。
- **并发控制**：引入消息发送队列（`_dispatch_queue`），优化入站消息的处理并发稳定性。

### Changed
- **消息识别优化**：改进 `is_group` 判断逻辑，支持更多种类的群聊标识符。
- **系统提示词更新**：优化对 AI 的指导建议，明确 MEDIA 路径在微信渠道的使用规范。

### Fixed
- 修复了断开连接时可能存在的任务清理不完全问题。
- 完善了媒体负载构建逻辑，支持自动推断 MIME 类型和文件名。
