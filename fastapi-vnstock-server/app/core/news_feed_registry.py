from __future__ import annotations

from dataclasses import dataclass
from typing import Final, Literal

NewsCategory = Literal["domestic", "world", "social"]


@dataclass(frozen=True, slots=True)
class NewsFeedSource:
    """A single RSS/Atom feed endpoint grouped for aggregation."""

    id: str
    display_name: str
    category: NewsCategory
    feed_url: str


# Curated RSS endpoints (finance / markets oriented). URLs may change; failures are reported per feed.
NEWS_FEED_SOURCES: Final[tuple[NewsFeedSource, ...]] = (
    # Trong nước
    NewsFeedSource(
        id="vnexpress_kinh_doanh",
        display_name="VnExpress Kinh doanh",
        category="domestic",
        feed_url="https://vnexpress.net/rss/kinh-doanh.rss",
    ),
    NewsFeedSource(
        id="cafebiz_home",
        display_name="CafeBiz",
        category="domestic",
        feed_url="https://cafebiz.vn/rss/home.rss",
    ),
    NewsFeedSource(
        id="thanhnien_kinh_te",
        display_name="Thanh Niên Kinh tế",
        category="domestic",
        feed_url="https://thanhnien.vn/rss/kinh-te.rss",
    ),
    NewsFeedSource(
        id="vietstock_thi_truong_ck",
        display_name="Vietstock Thị trường chứng khoán",
        category="domestic",
        feed_url="https://vietstock.vn/1328/dong-duong/thi-truong-chung-khoan.rss",
    ),
    NewsFeedSource(
        id="vietstock_nhan_dinh",
        display_name="Vietstock Nhận định thị trường",
        category="domestic",
        feed_url="https://vietstock.vn/1636/nhan-dinh-phan-tich/nhan-dinh-thi-truong.rss",
    ),
    # Thế giới
    NewsFeedSource(
        id="reuters_business",
        display_name="Reuters Business",
        category="world",
        feed_url="https://feeds.reuters.com/reuters/businessNews",
    ),
    NewsFeedSource(
        id="bbc_business",
        display_name="BBC Business",
        category="world",
        feed_url="https://feeds.bbci.co.uk/news/business/rss.xml",
    ),
    NewsFeedSource(
        id="bloomberg_markets",
        display_name="Bloomberg Markets",
        category="world",
        feed_url="https://feeds.bloomberg.com/markets/news.rss",
    ),
    NewsFeedSource(
        id="yahoo_news_top",
        display_name="Yahoo News (top)",
        category="world",
        feed_url="https://news.yahoo.com/rss/",
    ),
    # Social (public RSS where available; Reddit exposes .rss — Facebook/X need official APIs)
    NewsFeedSource(
        id="reddit_wallstreetbets",
        display_name="Reddit r/wallstreetbets",
        category="social",
        feed_url="https://www.reddit.com/r/wallstreetbets/hot.rss",
    ),
    NewsFeedSource(
        id="reddit_stocks",
        display_name="Reddit r/stocks",
        category="social",
        feed_url="https://www.reddit.com/r/stocks/hot.rss",
    ),
    NewsFeedSource(
        id="reddit_investing",
        display_name="Reddit r/investing",
        category="social",
        feed_url="https://www.reddit.com/r/investing/hot.rss",
    ),
    NewsFeedSource(
        id="reddit_securityanalysis",
        display_name="Reddit r/SecurityAnalysis",
        category="social",
        feed_url="https://www.reddit.com/r/SecurityAnalysis/hot.rss",
    ),
)

# Nguồn người dùng đặt tên nhưng không có RSS công khai ổn định trong registry hiện tại.
NEWS_SOURCES_PENDING_INTEGRATION: Final[tuple[str, ...]] = (
    "CafeF (cafef.vn): không có RSS công khai ổn định tại thời điểm tích hợp; có thể bổ sung khi có URL feed chính thức.",
    "Baomoi: không có RSS công khai trong registry; cần API/feed riêng từ nhà cung cấp.",
    "Tin nhanh chứng khoán: không có RSS công khai xác minh được; có thể thêm thủ công khi có URL.",
    "Fireant: dữ liệu qua ứng dụng/API riêng, không có RSS công khai trong phạm vi route này.",
    "Facebook và X (Twitter): cần Graph API / X API với khóa và tuân thủ điều khoản nền tảng.",
    "Gợi ý: khi feed RSS lỗi hoặc chưa có URL ổn định, dùng GET /news/firecrawl/discover (FIRECRAWL_API_KEY) để tìm lại URL RSS/feed qua Firecrawl Search, rồi xác minh tay trước khi thêm vào NEWS_FEED_SOURCES.",
)

# Gợi ý vận hành — trả về qua GET /news/sources.hints và GET /news khi có feed_errors
NEWS_API_HINTS: Final[dict[str, str]] = {
    "firecrawl_discover_rss": (
        "GET /news/firecrawl/discover — tìm gợi ý URL RSS/feed bằng Firecrawl Search "
        "(cần FIRECRAWL_API_KEY trong .env). Luôn kiểm tra URL trước khi thêm vào NEWS_FEED_SOURCES."
    ),
    "firecrawl_search_custom": (
        "GET /news/firecrawl/search?q=... — truy vấn Firecrawl tùy ý (site:, inurl:rss, ...)."
    ),
}
