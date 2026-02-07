from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import List, Optional
from datetime import datetime
from db import db
from cache import redis_cache, cache_response, invalidate_cache
from metrics import track_endpoint_metrics, increment_endpoint_counter, record_endpoint_duration
import asyncpg
import time

router = APIRouter(prefix="/tasks", tags=["Tasks"])

class TaskCreate(BaseModel):
    title: str
    description: Optional[str] = None
    status: Optional[str] = "active"

class TaskResponse(BaseModel):
    id: int
    title: str
    description: Optional[str] = None
    status: str
    created_at: datetime
    
    class Config:
        from_attributes = True

class TaskUpdate(BaseModel):
    title: Optional[str] = None
    description: Optional[str] = None
    status: Optional[str] = None

async def get_connection():
    if not db.pool:
        raise HTTPException(status_code=503, detail="Database not connected")
    return db.pool


@router.get("/cache/stats")
@track_endpoint_metrics("cache_stats", "GET")
async def get_cache_stats():
    start_time = time.time()
    increment_endpoint_counter('cache_stats_get')
    
    stats = await redis_cache.get_stats()
    
    duration = time.time() - start_time
    return stats

@router.post("/cache/clear")
@track_endpoint_metrics("cache_clear", "POST")
async def clear_cache():
    start_time = time.time()
    increment_endpoint_counter('cache_clear_post')
    
    if not redis_cache.enabled:
        raise HTTPException(status_code=400, detail="Cache is disabled")
    
    success = await redis_cache.clear_all()
    
    duration = time.time() - start_time
    if success:
        return {"message": "Cache cleared successfully"}
    raise HTTPException(status_code=500, detail="Failed to clear cache")

@router.get("/", response_model=List[TaskResponse])
@cache_response(ttl=60, key_prefix="tasks")
@track_endpoint_metrics("get_all_tasks", "GET")
async def get_all_tasks():
    start_time = time.time()
    increment_endpoint_counter('tasks_get_all')
    
    try:
        pool = await get_connection()
        async with pool.acquire() as conn:
            results = await conn.fetch(
                "SELECT id, title, description, status, created_at FROM tasks ORDER BY created_at DESC, id"
            )
            
            tasks = []
            for row in results:
                task_dict = {
                    "id": row["id"],
                    "title": row["title"],
                    "description": row["description"],
                    "status": row["status"],
                    "created_at": row["created_at"]
                }
                tasks.append(TaskResponse(**task_dict))
            
            duration = time.time() - start_time
            record_endpoint_duration('tasks_get_all', duration)
            return tasks
            
    except asyncpg.exceptions.PostgresError as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

@router.get("/{task_id}", response_model=TaskResponse)
@cache_response(ttl=120, key_prefix="task")
@track_endpoint_metrics("get_task_by_id", "GET")
async def get_task_by_id(task_id: int):
    start_time = time.time()
    increment_endpoint_counter('tasks_get_by_id', {'task_id': str(task_id)})
    
    try:
        pool = await get_connection()
        async with pool.acquire() as conn:
            result = await conn.fetchrow(
                "SELECT id, title, description, status, created_at FROM tasks WHERE id = $1",
                task_id
            )
            
            if not result:
                raise HTTPException(status_code=404, detail=f"Task with id {task_id} not found")
            
            task_dict = {
                "id": result["id"],
                "title": result["title"],
                "description": result["description"],
                "status": result["status"],
                "created_at": result["created_at"]
            }
            
            duration = time.time() - start_time
            record_endpoint_duration('tasks_get_by_id', duration)
            return TaskResponse(**task_dict)
            
    except asyncpg.exceptions.PostgresError as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

@router.post("/", response_model=TaskResponse, status_code=201)
@invalidate_cache(pattern="tasks:*")
@track_endpoint_metrics("create_task", "POST")
async def create_task(task: TaskCreate):
    start_time = time.time()
    increment_endpoint_counter('tasks_create')
    
    try:
        pool = await get_connection()
        async with pool.acquire() as conn:
            result = await conn.fetchrow(
                """
                INSERT INTO tasks (title, description, status)
                VALUES ($1, $2, $3)
                RETURNING id, title, description, status, created_at
                """,
                task.title, task.description, task.status
            )
            
            if not result:
                raise HTTPException(status_code=500, detail="Failed to create task")
            
            task_dict = {
                "id": result["id"],
                "title": result["title"],
                "description": result["description"],
                "status": result["status"],
                "created_at": result["created_at"]
            }
            
            if redis_cache.is_connected():
                await redis_cache.delete(f"task:get_task_by_id:{hash(str((task_dict['id'],)))}")
            
            duration = time.time() - start_time
            record_endpoint_duration('tasks_create', duration)
            return TaskResponse(**task_dict)
            
    except asyncpg.exceptions.UniqueViolationError:
        raise HTTPException(status_code=409, detail="Task already exists")
    except asyncpg.exceptions.PostgresError as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

@router.put("/{task_id}", response_model=TaskResponse)
@invalidate_cache(pattern="tasks:*")
@track_endpoint_metrics("update_task", "PUT")
async def update_task(task_id: int, task: TaskUpdate):
    start_time = time.time()
    increment_endpoint_counter('tasks_update', {'task_id': str(task_id)})
    
    try:
        pool = await get_connection()
        async with pool.acquire() as conn:
            current = await conn.fetchrow(
                "SELECT id, title, description, status FROM tasks WHERE id = $1",
                task_id
            )
            
            if not current:
                raise HTTPException(status_code=404, detail=f"Task with id {task_id} not found")
            
            update_fields = {}
            if task.title is not None:
                update_fields['title'] = task.title
            if task.description is not None:
                update_fields['description'] = task.description
            if task.status is not None:
                update_fields['status'] = task.status
                
            if not update_fields:
                raise HTTPException(status_code=400, detail="No fields to update")
            
            set_clause = ", ".join([f"{field} = ${i+2}" for i, field in enumerate(update_fields.keys())])
            values = list(update_fields.values())
            
            result = await conn.fetchrow(
                f"""
                UPDATE tasks 
                SET {set_clause}, updated_at = CURRENT_TIMESTAMP
                WHERE id = $1
                RETURNING id, title, description, status, created_at
                """,
                task_id, *values
            )
            
            task_dict = {
                "id": result["id"],
                "title": result["title"],
                "description": result["description"],
                "status": result["status"],
                "created_at": result["created_at"]
            }
            
            if redis_cache.is_connected():
                await redis_cache.delete(f"task:get_task_by_id:{hash(str((task_id,)))}")
            
            duration = time.time() - start_time
            record_endpoint_duration('tasks_update', duration)
            return TaskResponse(**task_dict)
            
    except asyncpg.exceptions.PostgresError as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

@router.delete("/{task_id}")
@invalidate_cache(pattern="tasks:*")
@track_endpoint_metrics("delete_task", "DELETE")
async def delete_task(task_id: int):
    start_time = time.time()
    increment_endpoint_counter('tasks_delete', {'task_id': str(task_id)})
    
    try:
        pool = await get_connection()
        async with pool.acquire() as conn:
            exists = await conn.fetchval(
                "SELECT EXISTS(SELECT 1 FROM tasks WHERE id = $1)",
                task_id
            )
            
            if not exists:
                raise HTTPException(status_code=404, detail=f"Task with id {task_id} not found")
            
            await conn.execute("DELETE FROM tasks WHERE id = $1", task_id)
            
            if redis_cache.is_connected():
                await redis_cache.delete(f"task:get_task_by_id:{hash(str((task_id,)))}")
            
            duration = time.time() - start_time
            record_endpoint_duration('tasks_delete', duration)
            return {"message": f"Task {task_id} deleted successfully"}
            
    except asyncpg.exceptions.PostgresError as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

@router.get("/status/{status}", response_model=List[TaskResponse])
@cache_response(ttl=90, key_prefix="tasks_by_status")
@track_endpoint_metrics("get_tasks_by_status", "GET")
async def get_tasks_by_status(status: str):
    increment_endpoint_counter('tasks_get_by_status', {'status': status})
    
    try:
        pool = await get_connection()
        async with pool.acquire() as conn:
            results = await conn.fetch(
                """
                SELECT id, title, description, status, created_at 
                FROM tasks 
                WHERE status = $1 
                ORDER BY created_at DESC
                """,
                status
            )
            
            tasks = []
            for row in results:
                task_dict = {
                    "id": row["id"],
                    "title": row["title"],
                    "description": row["description"],
                    "status": row["status"],
                    "created_at": row["created_at"]
                }
                tasks.append(TaskResponse(**task_dict))
            
            return tasks
            
    except asyncpg.exceptions.PostgresError as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

@router.get("/search/", response_model=List[TaskResponse])
@cache_response(ttl=120, key_prefix="tasks_search")
@track_endpoint_metrics("search_tasks", "GET")
async def search_tasks(q: str, limit: int = 10):
    increment_endpoint_counter('tasks_search')
    
    try:
        pool = await get_connection()
        async with pool.acquire() as conn:
            search_term = f"%{q}%"
            results = await conn.fetch(
                """
                SELECT id, title, description, status, created_at 
                FROM tasks 
                WHERE title ILIKE $1 OR description ILIKE $1
                ORDER BY created_at DESC
                LIMIT $2
                """,
                search_term, limit
            )
            
            tasks = []
            for row in results:
                task_dict = {
                    "id": row["id"],
                    "title": row["title"],
                    "description": row["description"],
                    "status": row["status"],
                    "created_at": row["created_at"]
                }
                tasks.append(TaskResponse(**task_dict))
            
            return tasks
            
    except asyncpg.exceptions.PostgresError as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")