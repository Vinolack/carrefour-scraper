import os
import toml

class config:
    def __init__(self):
        self.config_data = {}
        # 尝试加载配置文件作为默认值
        config_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "config.toml")
        if os.path.exists(config_path):
            try:
                with open(config_path, 'r', encoding='utf-8') as f:
                    self.config_data = toml.load(f)
            except: pass

    def get_key(self, key):
        # 优先读取环境变量 (Docker 最佳实践)
        env_val = os.getenv(key.upper())
        if env_val: return env_val
        
        if key == "api":
            return {
                "cf_host": os.getenv("CF_HOST", self.config_data.get("api", {}).get("cf_host", "127.0.0.1")),
                "cf_port": os.getenv("CF_PORT", self.config_data.get("api", {}).get("cf_port", 3000))
            }
        
        return self.config_data.get(key)