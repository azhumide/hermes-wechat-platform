# WeChat Platform Plugin for Hermes

This user plugin registers a Hermes gateway platform named `wechat`.

It connects Hermes to `aibot/plugins/HermesBridge` over WebSocket:

```text
WECHAT_BRIDGE_WS_URL=ws://127.0.0.1:9094/ws
```

The aibot side owns the real WeChat client and routing rules. Hermes only sees messages that `HermesBridge` forwards.

Enablement:

```yaml
plugins:
  enabled:
    - wechat-platform
```

Common environment variables:

```text
WECHAT_BRIDGE_WS_URL=ws://127.0.0.1:9094/ws
WECHAT_ALLOW_ALL_USERS=true
WECHAT_ALLOWED_USERS=wxid_9uwska6u4yzm22
WECHAT_HOME_CHANNEL=56816294015@chatroom
```

After changing these values, restart the Hermes gateway process so its
authorization cache and adapter process pick them up.
