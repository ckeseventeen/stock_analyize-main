"""
src/web/framework.py — Streamlit 页面框架

替代 12 个页面里手抄的：
  - sys.path.insert 样板（11/12 页都有）
  - st.set_page_config 样板（12/12 页都有）
  - setup_matplotlib_chinese 调用
  - st.title + st.caption 样板

用法：
    # src/web/pages/X_新页面.py
    from src.web.framework import Page

    class MyPage(Page):
        title = "我的工具"
        icon = "🛠️"
        caption = "随手做点事"

        def render(self):
            st.write("Hello")

    MyPage.run()    # 单行启动

或者使用更短的 @page 装饰器：
    from src.web.framework import page

    @page(title="我的工具", icon="🛠️")
    def render(ctx):
        st.write("Hello")
"""
from __future__ import annotations

import sys
from pathlib import Path
from typing import Any, Callable, ClassVar


def inject_project_path() -> Path:
    """
    把项目根目录加到 sys.path 顶部。

    自动定位：从当前文件向上找，遇到 main.py 或 pyproject.toml 即为项目根。
    替代 12 个页面里手抄的 5 行 sys.path.insert 样板。
    """
    here = Path(__file__).resolve()
    for parent in [here.parent] + list(here.parents):
        if (parent / "main.py").exists() or (parent / "pyproject.toml").exists():
            root = parent
            if str(root) not in sys.path:
                sys.path.insert(0, str(root))
            return root
    return here.parent


# Page 类用到的 streamlit 是延迟 import（防止纯单元测试不依赖 streamlit）
def _import_streamlit():
    import streamlit as st
    return st


class Page:
    """
    Streamlit 页面基类。

    子类只需声明 title/icon/caption 类属性 + 实现 render() 方法，
    或调用 Page.run() 启动。
    """

    title: ClassVar[str] = "页面"
    icon: ClassVar[str] = "📄"
    caption: ClassVar[str] = ""
    layout: ClassVar[str] = "wide"
    initial_sidebar_state: ClassVar[str] = "expanded"

    # 是否调用 setup_matplotlib_chinese（如果页面要画 matplotlib 图）
    setup_matplotlib: ClassVar[bool] = False

    def __init__(self):
        inject_project_path()
        st = _import_streamlit()
        # st.navigation 路由模式下由 app.py 统一 set_page_config，
        # 这里重复调用会抛异常 → 静默跳过（独立运行页面时仍生效）
        try:
            st.set_page_config(
                page_title=self.title,
                page_icon=self.icon,
                layout=self.layout,
                initial_sidebar_state=self.initial_sidebar_state,
            )
        except Exception:
            pass
        if self.setup_matplotlib:
            try:
                from src.web.utils import setup_matplotlib_chinese
                setup_matplotlib_chinese()
            except Exception:
                pass

        # 顶部标题（图标 + 标题）
        title_text = f"{self.icon} {self.title}" if self.icon else self.title
        st.title(title_text)
        if self.caption:
            st.caption(self.caption)

    def render(self) -> None:
        """子类实现：页面主体"""
        raise NotImplementedError(f"{type(self).__name__} 必须实现 render()")

    @classmethod
    def run(cls) -> None:
        """启动页面（实例化 + render）"""
        instance = cls()
        instance.render()


def page(
    *,
    title: str,
    icon: str = "📄",
    caption: str = "",
    layout: str = "wide",
    setup_matplotlib: bool = False,
) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """
    函数式装饰器：把一个普通 render() 函数包成一个 Page 子类并 run。

    @page(title="我的工具", icon="🛠️")
    def render():
        st.write("Hello")
    """
    def _decorator(render_fn: Callable[..., Any]) -> Callable[..., Any]:
        page_cls = type(
            f"_FunctionalPage_{render_fn.__name__}",
            (Page,),
            {
                "title": title,
                "icon": icon,
                "caption": caption,
                "layout": layout,
                "setup_matplotlib": setup_matplotlib,
                "render": lambda self: render_fn(),
            },
        )
        page_cls.run()
        return render_fn

    return _decorator
