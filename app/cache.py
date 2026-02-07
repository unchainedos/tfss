import redis.asyncio as redis
import json
import pickle
from typing import Optional, Any, Union
from functools import wraps
import asyncio
import os
from dotenv import load_dotenv
from datetime import timedelta

load_dotenv()

class RedisCache:
    def __init__(self):
        self.client: Optional[redis.Redis] = None
        self.ttl = int(os.getenv("REDIS_TTL", 300))  # 5 минут по умолчанию
        self.enabled = os.getenv("CACHE_ENABLED", "true").lower() == "true"
        
    async def connect(self):
        """Подключаемся к Redis"""
        if not self.enabled:
            print("Cache is disabled")
            return
            
        try:
            redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
            self.client = redis.from_url(
                redis_url,
                encoding="utf-8",
                decode_responses=False  # Мы сами будем декодировать
            )
            await self.client.ping()
            print("Redis cache connected")
        except Exception as e:
            print(f"Failed to connect to Redis: {e}")
            self.client = None
            
    async def disconnect(self):
        """Отключаемся от Redis"""
        if self.client:
            await self.client.close()
            self.client = None
            
    def is_connected(self) -> bool:
        """Проверяем подключение к Redis"""
        return self.client is not None and self.enabled
        
    async def set(self, key: str, value: Any, ttl: Optional[int] = None) -> bool:
        """Сохраняем значение в кэш"""
        if not self.is_connected():
            return False
            
        try:
            # Сериализуем значение
            serialized = pickle.dumps(value)
            
            # Устанавливаем TTL
            expire_time = ttl if ttl is not None else self.ttl
            
            result = await self.client.setex(
                key, 
                timedelta(seconds=expire_time), 
                serialized
            )
            return result is True
        except Exception as e:
            print(f"Cache set error: {e}")
            return False
            
    async def get(self, key: str) -> Optional[Any]:
        """Получаем значение из кэша"""
        if not self.is_connected():
            return None
            
        try:
            data = await self.client.get(key)
            if data:
                return pickle.loads(data)
            return None
        except Exception as e:
            print(f"Cache get error: {e}")
            return None
            
    async def delete(self, key: str) -> bool:
        """Удаляем значение из кэша"""
        if not self.is_connected():
            return False
            
        try:
            result = await self.client.delete(key)
            return result > 0
        except Exception as e:
            print(f"Cache delete error: {e}")
            return False
            
    async def delete_pattern(self, pattern: str) -> int:
        """Удаляем все ключи по паттерну"""
        if not self.is_connected():
            return 0
            
        try:
            keys = await self.client.keys(pattern)
            if keys:
                return await self.client.delete(*keys)
            return 0
        except Exception as e:
            print(f"Cache delete pattern error: {e}")
            return 0
            
    async def clear_all(self) -> bool:
        """Очищаем весь кэш"""
        if not self.is_connected():
            return False
            
        try:
            return await self.client.flushdb()
        except Exception as e:
            print(f"Cache clear error: {e}")
            return False
            
    async def get_stats(self) -> dict:
        """Получаем статистику кэша"""
        if not self.is_connected():
            return {"enabled": False}
            
        try:
            info = await self.client.info()
            return {
                "enabled": True,
                "connected": True,
                "keys": await self.client.dbsize(),
                "memory_used": info.get("used_memory_human", "N/A"),
                "hits": info.get("keyspace_hits", 0),
                "misses": info.get("keyspace_misses", 0),
                "hit_rate": info.get("keyspace_hits", 0) / 
                          max(1, info.get("keyspace_hits", 0) + info.get("keyspace_misses", 0))
            }
        except Exception as e:
            print(f"Cache stats error: {e}")
            return {"enabled": True, "connected": False, "error": str(e)}

# Декоратор для кэширования
def cache_response(ttl: Optional[int] = None, key_prefix: str = "cache"):
    """Декоратор для кэширования ответов API"""
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            # Создаем ключ кэша на основе функции и аргументов
            cache_key = f"{key_prefix}:{func.__name__}"
            
            # Добавляем аргументы в ключ
            if args:
                cache_key += f":{hash(str(args))}"
            if kwargs:
                cache_key += f":{hash(str(sorted(kwargs.items())))}"
                
            # Пытаемся получить из кэша
            cache = redis_cache
            if cache.is_connected():
                cached_result = await cache.get(cache_key)
                if cached_result is not None:
                    print(f"Cache hit: {cache_key}")
                    return cached_result
                print(f"Cache miss: {cache_key}")
                    
            # Выполняем функцию
            result = await func(*args, **kwargs)
            
            # Сохраняем в кэш
            if cache.is_connected() and result is not None:
                await cache.set(cache_key, result, ttl)
                
            return result
        return wrapper
    return decorator

# Декоратор для инвалидации кэша
def invalidate_cache(pattern: str = "*"):
    """Декоратор для инвалидации кэша после операции"""
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            # Выполняем функцию
            result = await func(*args, **kwargs)
            
            # Инвалидируем кэш
            cache = redis_cache
            if cache.is_connected():
                deleted = await cache.delete_pattern(pattern)
                print(f"Cache invalidated: {deleted} keys with pattern '{pattern}'")
                
            return result
        return wrapper
    return decorator

# Глобальный экземпляр кэша
redis_cache = RedisCache()