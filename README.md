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
WECHAT_BRIDGE_MEDIA_URL=http://10.10.10.80:9090
WECHAT_BRIDGE_MEDIA_TOKEN=<same value as AiBot BridgeMedia.access_token>
```

After changing these values, restart the Hermes gateway process so its
authorization cache and adapter process pick them up.

When the media service variables are configured, local files are uploaded to
AiBot over HTTP before being sent over the WebSocket. Incoming media URLs are
downloaded into Hermes' local cache, so the two machines do not need a shared
filesystem.
