from __future__ import annotations

from typing import Final, Literal

# Các domain báo / tài chính dùng cho Firecrawl Search (tin trong ngày, tbs=qdr:d).
DOMESTIC_PRESS_DOMAINS: Final[tuple[str, ...]] = (
    "vnexpress.net",
    "cafebiz.vn",
    "thanhnien.vn",
    "vietstock.vn",
    "baomoi.com",
    "cafef.vn",
    "tinnhanhchungkhoan.vn",
)

WORLD_PRESS_DOMAINS: Final[tuple[str, ...]] = (
    "reuters.com",
    "bbc.co.uk",
    "bbc.com",
    "bloomberg.com",
    "news.yahoo.com",
    "finance.yahoo.com",
)

# Ưu tiên chứng khoán / thị trường vốn; nhóm sau bắt các bài kinh tế chung vẫn liên quan TTCK.
_PRESS_KEYWORDS: Final[str] = (
    "("
    "chứng khoán OR cổ phiếu OR thị trường chứng khoán OR VN-Index OR HOSE OR HNX OR UPCOM "
    "OR thanh khoản OR khối ngoại OR tự doanh OR margin OR ETF OR IPO "
    "OR trái phiếu doanh nghiệp OR cổ tức OR giá cổ phiếu"
    ") OR ("
    "stock OR equities OR equity market OR shares OR IPO OR earnings "
    "OR kinh tế vĩ mô OR lãi suất OR Fed OR forex"
    ") OR ("
    "kinh tế OR thị trường OR finance OR business OR market"
    ")"
)

PressScope = Literal["all", "domestic", "world"]


def press_search_keywords_clause() -> str:
    """Cụm từ khóa OR dùng chung cho press today và tìm theo site: (RSS fallback)."""
    return _PRESS_KEYWORDS


def _domains_clause(domains: tuple[str, ...]) -> str:
    inner = " OR ".join(f"site:{host}" for host in domains)
    return f"({inner})"


def build_press_today_query(scope: PressScope) -> tuple[str, str | None]:
    """
    Trả về (query, country_hint).
    country_hint: gợi ý geo cho Firecrawl; None = để API mặc định.
    """
    if scope == "domestic":
        return (
            f"{_domains_clause(DOMESTIC_PRESS_DOMAINS)} {_PRESS_KEYWORDS}",
            "VN",
        )
    if scope == "world":
        return (
            f"{_domains_clause(WORLD_PRESS_DOMAINS)} {_PRESS_KEYWORDS}",
            "US",
        )
    combined = f"({_domains_clause(DOMESTIC_PRESS_DOMAINS)} OR {_domains_clause(WORLD_PRESS_DOMAINS)})"
    return (f"{combined} {_PRESS_KEYWORDS}", None)
