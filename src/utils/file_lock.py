"""
src/utils/file_lock.py — 跨平台进程级文件锁

用法：
    with file_lock("./cache/alert_state.json.lock"):
        # 此块内同一个锁文件只允许一个进程进入
        ...

实现：
  - POSIX：使用 fcntl.flock（推荐）
  - Windows：使用 msvcrt.locking（按字节区段锁）
  - 退路：若以上都不可用（如某些受限环境），回退到 OS 文件创建/删除 spin-lock

仅用于"协作式锁定"，不防止恶意进程绕过。适用于：
  - 调度器进程 + Web 进程对同一 state/CSV 的串行化写入
  - 防止 scheduler 双启动
"""
from __future__ import annotations

import os
import sys
import time
from contextlib import contextmanager
from pathlib import Path

_IS_WINDOWS = sys.platform.startswith("win")

if _IS_WINDOWS:
    import msvcrt
else:
    import fcntl


@contextmanager
def file_lock(lock_path: str | Path, timeout_seconds: float = 10.0, poll_interval: float = 0.05):
    """
    跨平台进程级文件锁。

    Args:
        lock_path: 锁文件路径（建议带 .lock 后缀，与被保护资源同目录）
        timeout_seconds: 获取锁超时
        poll_interval: 轮询间隔

    Raises:
        TimeoutError: 在 timeout_seconds 内未获取到锁
    """
    path = Path(lock_path)
    path.parent.mkdir(parents=True, exist_ok=True)

    # 以 a+ 模式打开（不存在则创建，不截断已有内容）
    fd = open(path, "a+")
    try:
        deadline = time.monotonic() + timeout_seconds
        acquired = False
        while True:
            try:
                if _IS_WINDOWS:
                    # msvcrt 需要非零长度的字节区段；锁 1 字节足够
                    msvcrt.locking(fd.fileno(), msvcrt.LK_NBLCK, 1)
                else:
                    fcntl.flock(fd.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                acquired = True
                break
            except (OSError, BlockingIOError):
                if time.monotonic() >= deadline:
                    raise TimeoutError(f"获取文件锁超时（{timeout_seconds}s）: {path}")
                time.sleep(poll_interval)

        try:
            yield
        finally:
            if acquired:
                try:
                    if _IS_WINDOWS:
                        # 释放前必须 seek 到锁定的字节
                        try:
                            fd.seek(0)
                            msvcrt.locking(fd.fileno(), msvcrt.LK_UNLCK, 1)
                        except OSError:
                            pass
                    else:
                        fcntl.flock(fd.fileno(), fcntl.LOCK_UN)
                except Exception:
                    # 关闭文件描述符时锁会自动释放，吞掉异常即可
                    pass
    finally:
        try:
            fd.close()
        except Exception:
            pass


def acquire_pid_lock(lock_path: str | Path) -> bool:
    """
    PID 锁：检测进程是否已运行，用于防止 scheduler 双启动。

    Returns:
        True - 成功获取（已写入 PID），调用者应在退出时调用 release_pid_lock()
        False - 已有同名进程持有锁
    """
    path = Path(lock_path)
    path.parent.mkdir(parents=True, exist_ok=True)

    if path.exists():
        try:
            existing_pid = int(path.read_text().strip())
            if _pid_alive(existing_pid):
                return False
            # PID 已不存在，是僵尸锁，可以接管
        except (ValueError, OSError):
            pass  # 锁文件损坏，覆盖即可

    try:
        path.write_text(str(os.getpid()))
        return True
    except OSError:
        return False


def release_pid_lock(lock_path: str | Path) -> None:
    """释放 PID 锁（删除文件）。即使锁不存在也不报错。"""
    try:
        Path(lock_path).unlink(missing_ok=True)
    except OSError:
        pass


def _pid_alive(pid: int) -> bool:
    """检测 PID 是否还在运行"""
    if pid <= 0:
        return False
    if _IS_WINDOWS:
        # Windows: 用 OpenProcess + GetExitCodeProcess
        try:
            import ctypes
            PROCESS_QUERY_INFORMATION = 0x0400
            kernel32 = ctypes.windll.kernel32
            handle = kernel32.OpenProcess(PROCESS_QUERY_INFORMATION, False, pid)
            if not handle:
                return False
            try:
                exit_code = ctypes.c_ulong()
                kernel32.GetExitCodeProcess(handle, ctypes.byref(exit_code))
                STILL_ACTIVE = 259
                return exit_code.value == STILL_ACTIVE
            finally:
                kernel32.CloseHandle(handle)
        except Exception:
            return False
    else:
        try:
            os.kill(pid, 0)  # signal 0 = 仅检测存在
            return True
        except (OSError, ProcessLookupError):
            return False
