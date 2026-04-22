"""
src/utils/exception_handler.py — 异常处理与重试工具

提供：
  - retry 装饰器：支持指数退避重试
  - safe_call：静默捕获异常，返回默认值
  - StockDataError / StrategyError：自定义异常体系
"""
import functools
import logging
import time
from typing import Any, Callable

logger = logging.getLogger("stock_analyzer")


# ========================
# 自定义异常体系
# ========================

class StockAnalyzeBaseError(Exception):
    """项目根异常，所有自定义异常的基类"""
    pass


class StockDataError(StockAnalyzeBaseError):
    """数据层异常：获取/清洗/存储失败"""
    pass


class NetworkError(StockDataError):
    """网络请求异常（akshare/pytdx 连接失败）"""
    pass


class DataParseError(StockDataError):
    """数据解析/格式转换异常"""
    pass


class StrategyError(StockAnalyzeBaseError):
    """策略/回测层异常"""
    pass


class FactorCalculationError(StockAnalyzeBaseError):
    """因子计算异常"""
    pass


class ConfigError(StockAnalyzeBaseError):
    """配置文件解析异常"""
    pass


class AlertError(StockAnalyzeBaseError):
    """预警推送异常"""
    pass


# ========================
# retry 装饰器
# ========================

def retry(
    max_attempts: int = 3,
    delay: float = 1.0,
    backoff: float = 2.0,
    exceptions: tuple[type[Exception], ...] = (Exception,),
    logger_name: str = "stock_analyzer",
) -> Callable:
    """
    重试装饰器，支持指数退避。

    Args:
        max_attempts: 最大重试次数（含首次调用）
        delay: 首次重试等待秒数
        backoff: 退避系数（每次等待时间 = delay * backoff^(attempt-1)）
        exceptions: 触发重试的异常类型元组
        logger_name: 日志记录器名称

    Example:
        @retry(max_attempts=3, delay=2.0, exceptions=(NetworkError,))
        def fetch_data(code: str) -> pd.DataFrame:
            ...
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            _logger = logging.getLogger(logger_name)
            last_exception = None
            wait = delay

            for attempt in range(1, max_attempts + 1):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    last_exception = e
                    if attempt < max_attempts:
                        _logger.warning(
                            f"[retry] {func.__qualname__} 第 {attempt}/{max_attempts} 次失败 "
                            f"({type(e).__name__}: {e})，{wait:.1f}s 后重试..."
                        )
                        time.sleep(wait)
                        wait *= backoff
                    else:
                        _logger.error(
                            f"[retry] {func.__qualname__} 已达最大重试次数 ({max_attempts})，"
                            f"最后错误: {e}"
                        )

            raise last_exception  # type: ignore

        return wrapper
    return decorator


# ========================
# safe_call 工具函数
# ========================

def safe_call(
    func: Callable,
    *args,
    default: Any = None,
    log_error: bool = True,
    **kwargs,
) -> Any:
    """
    安全调用函数，捕获所有异常并返回默认值。

    Args:
        func: 要调用的函数
        *args: 位置参数
        default: 出错时返回的默认值
        log_error: 是否记录错误日志
        **kwargs: 关键字参数

    Returns:
        函数返回值，或 default（出错时）

    Example:
        result = safe_call(risky_function, arg1, default=pd.DataFrame())
    """
    try:
        return func(*args, **kwargs)
    except Exception as e:
        if log_error:
            logger.error(f"safe_call: {func.__qualname__} 执行失败: {e}", exc_info=True)
        return default
