# 前端 UX/逻辑重构方案

> 审计范围：13 个页面，51 项问题（4 Critical · 12 High · 20 Medium · 15 Low）

---

## 一、跨页面共性改进（基础设施层，一次修复全局生效）

### 1. 状态持久化框架 `src/web/state.py`

**问题**：分析结果在切换参数/页面后消失；估值页切换股票仍显示旧结果

**方案**：统一 `session_state` 管理器

```python
# src/web/state.py
import streamlit as st
from dataclasses import dataclass, asdict
from typing import Any
import hashlib, json

@dataclass
class AnalysisState:
    """统一分析结果状态"""
    stock_code: str
    market: str
    params_hash: str   # 参数指纹，检测 stale
    result: Any
    timestamp: float

def _params_hash(params: dict) -> str:
    return hashlib.md5(json.dumps(params, sort_keys=True, default=str).encode()).hexdigest()[:8]

def save_result(key: str, stock_code: str, market: str, params: dict, result: Any):
    """保存分析结果到 session_state"""
    st.session_state[key] = AnalysisState(
        stock_code=stock_code,
        market=market,
        params_hash=_params_hash(params),
        result=result,
        timestamp=st.runtime.scriptrunner.add_script_run_ctx().time if hasattr(st.runtime, 'scriptrunner') else 0,
    )

def load_result(key: str, stock_code: str, market: str, params: dict) -> Any | None:
    """加载结果，自动检测 stale"""
    state = st.session_state.get(key)
    if state is None:
        return None
    if state.stock_code != stock_code or state.market != market:
        return None  # 股票变了，结果作废
    if state.params_hash != _params_hash(params):
        return None  # 参数变了，结果作废
    return state.result

def is_stale(key: str, stock_code: str, market: str, params: dict) -> bool:
    """检查当前显示的结果是否与当前参数不匹配"""
    state = st.session_state.get(key)
    if state is None:
        return True
    return (state.stock_code != stock_code
            or state.market != market
            or state.params_hash != _params_hash(params))
```

**影响页面**：估值分析、FCF分析、策略回测 — 三个页面的结果持久化统一用这套

### 2. 确认对话框组件 `src/web/components/confirm.py`

**问题**：10+ 处删除/停止操作无二次确认

**方案**：可复用的确认弹窗组件

```python
# src/web/components/confirm.py
import streamlit as st

def confirm_action(key: str, message: str, button_label: str = "确认执行", danger: bool = True) -> bool | None:
    """
    两步确认组件:
    1. 第一次点击 → 显示确认消息
    2. 第二次点击 → 返回 True
    3. 取消 → 返回 False
    未触发 → 返回 None
    """
    state_key = f"_confirm_{key}"
    if st.button(button_label, key=f"_btn_{key}", type="primary" if not danger else "secondary"):
        st.session_state[state_key] = True

    if st.session_state.get(state_key):
        color = "#dc3545" if danger else "#ffc107"
        st.markdown(f'<p style="color:{color};font-weight:600;">⚠️ {message}</p>', unsafe_allow_html=True)
        col1, col2 = st.columns(2)
        with col1:
            if st.button("✓ 确认", key=f"_yes_{key}"):
                st.session_state[state_key] = False
                return True
        with col2:
            if st.button("✗ 取消", key=f"_no_{key}"):
                st.session_state[state_key] = False
                return False
    return None
```

**影响页面**：策略配置(delete strategy/condition)、价格预警(delete rule)、调度管理(stop scheduler/pause job)、关注标的(delete stock/batch delete)

### 3. 未保存变更提示 `src/web/components/unsaved.py`

**问题**：策略配置编辑后切换页面即丢失，无任何提示

**方案**：对比原始数据，显示 unsaved badge

```python
# src/web/components/unsaved.py
import streamlit as st
import yaml

def mark_dirty(key: str):
    """标记为有未保存变更"""
    st.session_state[f"_dirty_{key}"] = True

def mark_clean(key: str):
    """标记为已保存"""
    st.session_state[f"_dirty_{key}"] = False

def is_dirty(key: str) -> bool:
    return st.session_state.get(f"_dirty_{key}", False)

def unsaved_badge(key: str):
    """显示未保存提示"""
    if is_dirty(key):
        st.markdown(
            '<div style="background:#fff3cd;color:#856404;padding:8px 16px;border-radius:6px;font-size:13px;">'
            '⚠️ 有未保存的变更 — 离开此页面将丢失'
            '</div>', unsafe_allow_html=True)
```

**影响页面**：策略配置（主要）、配置管理

### 4. 统一分页组件 `src/web/components/pagination.py`

**问题**：所有 DataFrame 全量渲染，大数据集卡顿

**方案**：

```python
# src/web/components/pagination.py
import streamlit as st
import pandas as pd

def paginated_dataframe(df: pd.DataFrame, page_size: int = 50, key: str = "pager"):
    """带分页的 DataFrame 展示"""
    total = len(df)
    if total == 0:
        st.info("暂无数据")
        return

    total_pages = (total - 1) // page_size + 1
    page = st.number_input("页码", min_value=1, max_value=total_pages, value=1, key=f"{key}_page")
    start = (page - 1) * page_size
    end = min(start + page_size, total)

    st.dataframe(df.iloc[start:end], use_container_width=True)
    st.caption(f"显示 {start+1}-{end} / 共 {total} 条 · 第 {page}/{total_pages} 页")
```

**影响页面**：股票筛选结果、告警历史、关注标的、策略配置条件列表

### 5. 搜索/过滤组件 `src/web/components/search.py`

**问题**：下拉框50+股票无法搜索；关注标的搜索不支持模糊匹配

**方案**：Streamlit 的 `st.selectbox` 不支持搜索，但可以用 `st.text_input` + 过滤列表实现

```python
# src/web/components/search.py
import streamlit as st
from typing import Callable

def searchable_select(label: str, options: list[dict], key: str,
                      id_field: str = "code", name_field: str = "name"):
    """
    可搜索的下拉选择器
    options: [{"code": "600519", "name": "贵州茅台"}, ...]
    返回: 选中的 id (code)
    """
    query = st.text_input(f"🔍 {label}", key=f"{key}_search", placeholder="输入代码或名称搜索...")

    if query:
        q = query.lower()
        filtered = [o for o in options if q in str(o.get(id_field, "")).lower()
                    or q in str(o.get(name_field, "")).lower()]
    else:
        filtered = options

    if not filtered:
        st.warning("无匹配结果")
        return None

    display = [f"{o[id_field]} - {o[name_field]}" for o in filtered]
    selected_idx = st.selectbox(label, range(len(display)),
                                format_func=lambda i: display[i],
                                key=f"{key}_select")
    return filtered[selected_idx][id_field]
```

---

## 二、页面级重构方案

### P1 估值分析 — 3 项改动

| 改动 | 问题 | 方案 |
|------|------|------|
| **搜索选股** | 下拉框50+股票无法搜索 | 用 `searchable_select` 替代 `st.selectbox` |
| **Stale 结果警告** | 切换股票后仍显示旧结果 | 用 `state.py` 的 `is_stale()` 检测，显示黄色横幅"当前显示的是 XXX 的分析结果，与您选择的 YYY 不匹配" |
| **PE/PS 同时展示** | 只能二选一 | 改为两个 tab 或两栏并排 |

### P2 策略配置 — 5 项改动

| 改动 | 问题 | 方案 |
|------|------|------|
| **自动保存** | 编辑不保存即丢失 | 每次变更自动 `atomic_save_yaml`，同时保留"撤销"功能 |
| **未保存提示** | 无 indicator | 用 `unsaved_badge` |
| **删除确认** | 点击即删无确认 | 用 `confirm_action` |
| **条件列表分页** | 20+ 条件全展开 | 用 `paginated_dataframe` 展示条件列表概览，点击进入编辑 |
| **条件类型搜索** | 20+ 类型按钮难以扫描 | 用 `searchable_select` 替代按钮列表 |

### P4 策略回测 — 4 项改动

| 改动 | 问题 | 方案 |
|------|------|------|
| **侧栏拆分** | 20+ 控件无法一屏看完 | 基本参数(market/code/days/cash)留在 sidebar；策略配置 + 高级参数移到主区域 `st.tabs(["基本参数", "策略配置", "高级设置"])` |
| **结果持久化** | 切换参数结果消失 | 用 `state.py` 保存 |
| **YAML编辑器改进** | 窄文本框编辑YAML痛苦 | 改为主区域的 `st.text_area(height=500)`，或提供结构化表单替代 |
| **回测对比** | 新回测覆盖旧结果 | 保存最近 3 次回测结果，用 tab 切换对比 |

### P5 价格预警 — 3 项改动

| 改动 | 问题 | 方案 |
|------|------|------|
| **多条件规则** | 只能单条件 | 添加条件列表，支持 AND/OR 逻辑 |
| **规则编辑** | 只能删+重建 | 每条规则增加编辑按钮，弹出表单 |
| **规则选择删除** | 输入索引号删除 | 用 `st.dataframe` + `on_select` 回调，点击行选中后删除 |

### P8 告警历史 — 2 项改动

| 改动 | 问题 | 方案 |
|------|------|------|
| **日期范围过滤** | 只能按代码/类型过滤 | 增加 `st.date_input` 日期范围选择器 |
| **单条删除** | 只能批量清理 | 每行增加"忽略"按钮，标记为已处理 |

### P9/P12 关注标的去重 — 合并为一个页面

**问题**：P9 配置管理的 Watchlist tab 和 P12 关注标的管理同一份数据

**方案**：
- 保留 P12 关注标的作为唯一入口（功能更完整）
- P9 配置管理中删除 Watchlist tab，改为跳转链接 `st.page_link("pages/12_关注标的.py", label="前往关注标的管理 →")`
- P9 专注于指标/因子/预设管理

### P10 调度管理 — 3 项改动

| 改动 | 问题 | 方案 |
|------|------|------|
| **停止确认** | 一键停全部无确认 | 用 `confirm_action` |
| **操作不刷新** | trigger/pause/resume 导致全页重渲染 | 改用 `st.fragment` (Streamlit 1.37+) 隔离操作区域 |
| **执行历史翻页** | 限制50条 | 用 `paginated_dataframe` |

### P11 FCF分析 — 3 项改动

| 改动 | 问题 | 方案 |
|------|------|------|
| **数据缓存** | 每次都联网获取 | 加 `@st.cache_data(ttl=300)` |
| **结果持久化** | 切换参数结果消失 | 用 `state.py` |
| **加入关注** | 无关注列表功能 | 添加 `quick_add_stock_widget` |

### P12 关注标的 — 3 项改动

| 改动 | 问题 | 方案 |
|------|------|------|
| **行业列表可扩展** | 硬编码37个行业 | 从 YAML 动态加载 + 支持用户自定义添加 |
| **市场切换保持状态** | 切换市场重置所有 | 用 `st.session_state` 保持当前 tab 和搜索状态 |
| **模糊搜索** | 中文搜索不支持模糊 | 用 `fuzzywuzzy` 或简单的前缀+子串匹配 |

---

## 三、实施优先级

### Phase 1 — 基础设施（1-2天）

1. 创建 `src/web/state.py` — 统一状态管理
2. 创建 `src/web/components/confirm.py` — 确认对话框
3. 创建 `src/web/components/unsaved.py` — 未保存提示
4. 创建 `src/web/components/pagination.py` — 分页组件
5. 创建 `src/web/components/search.py` — 搜索组件

### Phase 2 — Critical 修复（2-3天）

1. P2 策略配置 — 自动保存 + 未保存提示 + 删除确认
2. P4 策略回测 — 侧栏拆分 + 结果持久化
3. P1 估值分析 — Stale 结果警告 + 搜索选股
4. P9/P12 去重 — 删除 P9 Watchlist tab

### Phase 3 — High 修复（2-3天）

1. P5 价格预警 — 多条件规则 + 规则编辑
2. P10 调度管理 — 停止确认 + fragment 隔离
3. P11 FCF分析 — 缓存 + 持久化 + 关注列表
4. P12 关注标的 — 动态行业 + 模糊搜索

### Phase 4 — Medium 修复（2天）

1. 各页面分页组件接入
2. 搜索组件替换原有 selectbox
3. 告警历史日期过滤 + 单条删除
4. FCF/回测结果导出功能

---

## 四、设计原则总结

| 原则 | 当前 | 目标 |
|------|------|------|
| **状态持久化** | 结果随 rerun 消失 | session_state 自动保存 + stale 检测 |
| **操作安全** | 删除无确认 | 所有破坏性操作二次确认 |
| **信息密度** | 侧栏20+控件堆叠 | 按使用频率分区(tabs/accordion) |
| **可发现性** | 功能埋在嵌套菜单 | 搜索/过滤/快捷入口 |
| **一致性** | 各页面独立实现 | 统一组件库(components/) |
