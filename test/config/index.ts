import { ConfigManager, RedisConfig } from '@diff./config-manager';

const config = new ConfigManager<RedisConfig>();
config.setDevelopmentConfig({
  redis: {
    hosts: {
      tm: {
        host: '127.0.0.1',
        port: 6379
      }
    }
  }
});

export default config;
