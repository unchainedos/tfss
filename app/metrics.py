from prometheus_client import Counter, Histogram, Gauge, generate_latest, REGISTRY
from prometheus_client.multiprocess import MultiProcessCollector
import time
from functools import wraps
import os
from typing import Callable

if os.environ.get('prometheus_multiproc_dir'):
    registry = REGISTRY
else:
    from prometheus_client import CollectorRegistry
    registry = CollectorRegistry()
    if os.environ.get('PROMETHEUS_MULTIPROC_DIR'):
        MultiProcessCollector(registry)

tasks_get_all_total = Counter(
    'tasks_get_all_total',
    'Total GET requests to get all tasks',
    registry=registry
)

tasks_get_all_duration = Histogram(
    'tasks_get_all_duration_seconds',
    'Duration of GET /api/v1/tasks/ requests',
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5),
    registry=registry
)

tasks_get_by_id_total = Counter(
    'tasks_get_by_id_total',
    'Total GET requests to get task by ID',
    ['task_id'],
    registry=registry
)

tasks_get_by_id_duration = Histogram(
    'tasks_get_by_id_duration_seconds',
    'Duration of GET /api/v1/tasks/{id} requests',
    buckets=(0.001, 0.005, 0.01, 0.025, 0.05, 0.1),
    registry=registry
)
tasks_create_total = Counter(
    'tasks_create_total',
    'Total POST requests to create task',
    registry=registry
)

tasks_create_duration = Histogram(
    'tasks_create_duration_seconds',
    'Duration of POST /api/v1/tasks/ requests',
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5),
    registry=registry
)

tasks_update_total = Counter(
    'tasks_update_total',
    'Total PUT requests to update task',
    ['task_id'],
    registry=registry
)

tasks_update_duration = Histogram(
    'tasks_update_duration_seconds',
    'Duration of PUT /api/v1/tasks/{id} requests',
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25),
    registry=registry
)

tasks_delete_total = Counter(
    'tasks_delete_total',
    'Total DELETE requests to delete task',
    ['task_id'],
    registry=registry
)

tasks_delete_duration = Histogram(
    'tasks_delete_duration_seconds',
    'Duration of DELETE /api/v1/tasks/{id} requests',
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1),
    registry=registry
)

tasks_get_by_status_total = Counter(
    'tasks_get_by_status_total',
    'Total GET requests to get tasks by status',
    ['status'],
    registry=registry
)

tasks_search_total = Counter(
    'tasks_search_total',
    'Total GET requests to search tasks',
    registry=registry
)

cache_stats_get_total = Counter(
    'cache_stats_get_total',
    'Total GET requests to get cache stats',
    registry=registry
)

cache_clear_post_total = Counter(
    'cache_clear_post_total',
    'Total POST requests to clear cache',
    registry=registry
)

health_check_total = Counter(
    'health_check_total',
    'Total GET requests to health check',
    registry=registry
)

health_check_duration = Histogram(
    'health_check_duration_seconds',
    'Duration of GET /health requests',
    buckets=(0.001, 0.005, 0.01, 0.025, 0.05),
    registry=registry
)
root_total = Counter(
    'root_total',
    'Total GET requests to root endpoint',
    registry=registry
)

tasks_responses_total = Counter(
    'tasks_responses_total',
    'Total responses for tasks endpoints',
    ['endpoint', 'method', 'status_code'],
    registry=registry
)

tasks_errors_total = Counter(
    'tasks_errors_total',
    'Total errors for tasks endpoints',
    ['endpoint', 'error_type'],
    registry=registry
)

endpoint_response_time = Histogram(
    'endpoint_response_time_seconds',
    'Response time per endpoint',
    ['endpoint', 'method'],
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0),
    registry=registry
)
requests_in_progress = Gauge(
    'requests_in_progress',
    'Current requests in progress',
    ['endpoint'],
    registry=registry
)

def track_endpoint_metrics(endpoint_name: str, method: str = "GET"):
    def decorator(func: Callable):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            requests_in_progress.labels(endpoint=endpoint_name).inc()
            
            start_time = time.time()
            
            try:
                response = await func(*args, **kwargs)
                
                duration = time.time() - start_time
                
                endpoint_response_time.labels(
                    endpoint=endpoint_name,
                    method=method
                ).observe(duration)
                tasks_responses_total.labels(
                    endpoint=endpoint_name,
                    method=method,
                    status_code=200
                ).inc()
                
                return response
                
            except Exception as e:
                duration = time.time() - start_time
                
                endpoint_response_time.labels(
                    endpoint=endpoint_name,
                    method=method
                ).observe(duration)
                
                status_code = 500
                if hasattr(e, 'status_code'):
                    status_code = e.status_code
                
                tasks_responses_total.labels(
                    endpoint=endpoint_name,
                    method=method,
                    status_code=status_code
                ).inc()
                
                tasks_errors_total.labels(
                    endpoint=endpoint_name,
                    error_type=type(e).__name__
                ).inc()
                
                raise
                
            finally:
                requests_in_progress.labels(endpoint=endpoint_name).dec()
                
        return wrapper
    return decorator


def increment_endpoint_counter(counter_name: str, labels: dict = None):
    counters = {
        'tasks_get_all': tasks_get_all_total,
        'tasks_get_by_id': tasks_get_by_id_total,
        'tasks_create': tasks_create_total,
        'tasks_update': tasks_update_total,
        'tasks_delete': tasks_delete_total,
        'tasks_get_by_status': tasks_get_by_status_total,
        'tasks_search': tasks_search_total,
        'cache_stats_get': cache_stats_get_total,
        'cache_clear_post': cache_clear_post_total,
        'health_check': health_check_total,
        'root': root_total,
    }
    
    if counter_name in counters:
        counter = counters[counter_name]
        if labels:
            counter.labels(**labels).inc()
        else:
            counter.inc()

def record_endpoint_duration(counter_name: str, duration: float):
    histograms = {
        'tasks_get_all': tasks_get_all_duration,
        'tasks_get_by_id': tasks_get_by_id_duration,
        'tasks_create': tasks_create_duration,
        'tasks_update': tasks_update_duration,
        'tasks_delete': tasks_delete_duration,
        'health_check': health_check_duration,
    }
    
    if counter_name in histograms:
        histograms[counter_name].observe(duration)


def get_metrics():
    return generate_latest(registry)