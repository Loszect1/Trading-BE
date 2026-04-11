from __future__ import annotations

from dataclasses import dataclass
from typing import Final, Literal

DiscoveryPreset = Literal["domestic", "world", "all"]


@dataclass(frozen=True, slots=True)
class FirecrawlDiscoveryQuery:
    """Tập truy vấn web search (Firecrawl) để tìm URL RSS/feed công khai."""

    id: str
    query: str
    #: Mã quốc gia Firecrawl (ví dụ VN, US); rỗng = mặc định API.
    country: str
    preset: DiscoveryPreset


# Gợi ý truy vấn: kết quả phụ thuộc chỉ mục; cần kiểm tra tay URL trước khi đưa vào NEWS_FEED_SOURCES.
FIRECRAWL_DISCOVERY_QUERIES: Final[tuple[FirecrawlDiscoveryQuery, ...]] = (
    FirecrawlDiscoveryQuery(
        id="cafef_rss",
        query='site:cafef.vn (inurl:rss OR inurl:feed OR "rss")',
        country="VN",
        preset="domestic",
    ),
    FirecrawlDiscoveryQuery(
        id="baomoi_rss",
        query='site:baomoi.com (rss OR feed OR "tin-moi")',
        country="VN",
        preset="domestic",
    ),
    FirecrawlDiscoveryQuery(
        id="tinnhanhchungkhoan_rss",
        query='site:tinnhanhchungkhoan.vn (rss OR feed OR xml)',
        country="VN",
        preset="domestic",
    ),
    FirecrawlDiscoveryQuery(
        id="fireant_feed",
        query='site:fireant.vn (rss OR feed OR "tin tức" OR blog)',
        country="VN",
        preset="domestic",
    ),
    FirecrawlDiscoveryQuery(
        id="vnexpress_rss_verify",
        query="site:vnexpress.net rss kinh doanh",
        country="VN",
        preset="domestic",
    ),
    FirecrawlDiscoveryQuery(
        id="reuters_rss",
        query="reuters business rss feed url",
        country="US",
        preset="world",
    ),
    FirecrawlDiscoveryQuery(
        id="bloomberg_rss",
        query="bloomberg markets rss feed",
        country="US",
        preset="world",
    ),
    FirecrawlDiscoveryQuery(
        id="yahoo_finance_rss",
        query="yahoo finance news rss",
        country="US",
        preset="world",
    ),
)
