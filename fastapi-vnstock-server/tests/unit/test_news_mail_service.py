from __future__ import annotations

import json
from datetime import date
from uuid import uuid4


def test_extract_section_links_prefers_html_anchor_text_and_dedupes():
    from app.services.news_mail_service import extract_section_links

    rows = extract_section_links(
        html_text="""
        <html><body>
          <a href="https://example.vn/a">FPT wins contract</a>
          <a href="https://example.vn/a">Duplicate</a>
          <a href="https://example.vn/b?x=1">Banking sector note</a>
        </body></html>
        """,
        text="https://fallback.test/unused",
    )

    assert [row["url"] for row in rows] == ["https://example.vn/a", "https://example.vn/b?x=1"]
    assert rows[0]["section_index"] == 1
    assert rows[0]["section_title"] == "FPT wins contract"


def test_extract_section_links_uses_plaintext_urls_when_html_has_no_links():
    from app.services.news_mail_service import extract_section_links

    rows = extract_section_links(
        html_text="<p>No anchors</p>",
        text="Read https://example.vn/a, then https://example.vn/b.",
    )

    assert [row["url"] for row in rows] == ["https://example.vn/a", "https://example.vn/b"]


def test_extract_section_links_prefers_mail_link_field_over_trich_dan_anchor():
    from app.services.news_mail_service import extract_section_links

    truncated = "https://news.google.com/rss/articles/CBMipwFBVV95cUxOeXdQWDR0b25ORDJVWUkzWGpjTE"
    full = (
        "https://news.google.com/rss/articles/"
        "CBMipwFBVV95cUxOeXdQWDR0b25ORDJVWUkzWGpjTEVWZFBQc2RLZDlHR1V1ZHhvMFg5WWdXZjM5Z3pt"
        "?oc=5"
    )
    rows = extract_section_links(
        html_text=f'<a href="{truncated}">trich dan</a>',
        text=(
            "### [lai suat huy dong] Lai suat huy dong ngan hang hom nay\n"
            f'- **Trich dan:** <a href="{truncated}\n'
            f"- **Link:** {full}\n"
        ),
    )

    assert [row["url"] for row in rows] == [full]
    assert rows[0]["section_title"].startswith("[lai suat huy dong]")


def test_extract_section_links_joins_soft_wrapped_cafef_urls():
    from app.services.news_mail_service import extract_section_links

    rows = extract_section_links(
        html_text="<p>No anchors</p>",
        text=(
            "Read "
            "https://cafef.vn/loat-doanh-nghiep-duoc-bi-xu-phat-muc-cao-nhat-310-trieu-do\n"
            "ng-188260531090104584.chn"
        ),
    )

    assert [row["url"] for row in rows] == [
        "https://cafef.vn/loat-doanh-nghiep-duoc-bi-xu-phat-muc-cao-nhat-310-trieu-dong-188260531090104584.chn"
    ]


def test_extract_section_links_preserves_url_after_old_mail_text_cutoff():
    from app.services.news_mail_service import extract_section_links

    url = "https://cafef.vn/loat-doanh-nghiep-duoc-bi-xu-phat-muc-cao-nhat-310-trieu-dong-188260531090104584.chn"
    text = ("x" * 11990) + url

    rows = extract_section_links(html_text="<p>No anchors</p>", text=text)

    assert rows[-1]["url"] == url


def test_extract_section_links_does_not_merge_next_plain_sentence():
    from app.services.news_mail_service import extract_section_links

    rows = extract_section_links(
        html_text="<p>No anchors</p>",
        text="Read https://example.vn/a,\nnext-section has no link.",
    )

    assert [row["url"] for row in rows] == ["https://example.vn/a"]


def test_clean_article_html_removes_scripts_and_compacts_text(monkeypatch):
    import app.services.news_mail_service as svc

    monkeypatch.setattr(svc.settings, "news_mail_article_text_max_chars", 120)

    cleaned = svc.clean_article_html(
        """
        <html><head><title>Market title</title><script>alert(1)</script></head>
        <body><h1>FPT</h1><p>Positive contract news for technology exports.</p></body></html>
        """
    )

    assert cleaned["title"] == "Market title"
    assert "alert" not in cleaned["text"]
    assert "Positive contract news" in cleaned["text"]


def test_fetch_article_retries_anti_bot_challenge_with_browser_headers(monkeypatch):
    import httpx
    import app.services.news_mail_service as svc

    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if len(requests) == 1:
            return httpx.Response(
                401,
                headers={"server": "CloudFront", "content-type": "text/html;charset=utf-8"},
                text="Please enable JS and disable any ad blocker",
            )
        return httpx.Response(
            200,
            headers={"content-type": "text/html; charset=utf-8"},
            text="<html><head><title>Dow vs S&P</title></head><body>Asset allocation matters more.</body></html>",
        )

    transport = httpx.MockTransport(handler)
    real_client = svc.httpx.Client

    def fake_client(*args, **kwargs):
        kwargs["transport"] = transport
        return real_client(*args, **kwargs)

    monkeypatch.setattr(svc.httpx, "Client", fake_client)

    result = svc._fetch_article("https://www.marketwatch.com/story/test?.tsrc=rss")

    assert result["fetch_status"] == "fetched"
    assert result["http_status"] == 200
    assert result["title"] == "Dow vs S&P"
    assert len(requests) == 2
    assert "Windows NT 10.0" in requests[0].headers["user-agent"]
    assert requests[1].headers["referer"] == "https://finance.yahoo.com/"
    assert [item["status_code"] for item in result["metadata"]["fetch_attempts"]] == [401, 200]


def test_fetch_article_marks_datadome_block_after_retry(monkeypatch):
    import httpx
    import app.services.news_mail_service as svc

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            403,
            headers={"server": "DataDome", "content-type": "text/html;charset=utf-8"},
            text="Please enable cookies. DataDome anti-bot challenge.",
        )

    transport = httpx.MockTransport(handler)
    real_client = svc.httpx.Client

    def fake_client(*args, **kwargs):
        kwargs["transport"] = transport
        return real_client(*args, **kwargs)

    monkeypatch.setattr(svc.httpx, "Client", fake_client)

    result = svc._fetch_article("https://www.thestreet.com/investing/stocks/test?.tsrc=rss")

    assert result["fetch_status"] == "fetch_failed"
    assert result["http_status"] == 403
    assert result["fetch_error"] == "HTTP 403 blocked_by_datadome"
    assert result["metadata"]["blocked_by"] == "datadome"
    assert [item["status_code"] for item in result["metadata"]["fetch_attempts"]] == [403, 403]


def test_build_codex_prompt_preserves_article_ids():
    from app.services.news_mail_service import build_codex_prompt

    prompt = build_codex_prompt(
        [
            {
                "article_id": "article-1",
                "title": "FPT news",
                "url": "https://example.vn/fpt",
                "fetch_status": "fetched",
                "article_text": "FPT signed a new contract.",
            }
        ]
    )

    assert "article-1" in prompt
    assert "strict JSON" in prompt
    assert "FPT signed a new contract" in prompt
    assert "Vietnamese" in prompt
    assert "article_id" in prompt


def test_build_codex_prompt_includes_failed_fetch_context():
    from app.services.news_mail_service import build_codex_prompt

    prompt = build_codex_prompt(
        [
            {
                "article_id": "article-404",
                "title": "Missing CafeF article",
                "url": "https://cafef.vn/missing",
                "source_host": "cafef.vn",
                "fetch_status": "fetch_failed",
                "fetch_error": "HTTP 404",
                "article_text": "",
            }
        ]
    )

    assert "article-404" in prompt
    assert "HTTP 404" in prompt
    assert "Return one output item for every input article_id" in prompt


def test_news_mail_filter_normalizers_are_dashboard_safe():
    import app.services.news_mail_service as svc

    assert svc._normalize_news_mail_category_filter("Oil Gas") == "oil_gas"
    assert svc._normalize_news_mail_category_filter(" IPO ") == "ipo"
    assert svc._normalize_news_mail_category_filter("Ngân hàng") == "ngan_hang"
    assert svc._normalize_news_mail_symbol_filter(" vcb.vn ") == "VCBVN"
    assert svc._normalize_news_mail_symbol_filter("SSI") == "SSI"


def test_news_mail_category_maps_symbol_industry_to_dashboard_bucket():
    import app.services.news_mail_service as svc

    assert svc._category_from_industry_name("Ngân hàng") == ("Ngân hàng", "ngan_hang")
    assert svc._category_from_industry_name("SX Nhựa - Hóa chất") == ("Hóa chất", "hoa_chat")
    assert svc._category_from_industry_name("Dịch vụ lưu trú, ăn uống, giải trí") == (
        "Du lịch và Giải trí",
        "du_lich_va_giai_tri",
    )


def test_news_mail_article_category_uses_top_symbol_world_and_general():
    import app.services.news_mail_service as svc

    assert svc._news_mail_article_category_from_symbols([], {}) == ("General", "general")
    assert svc._news_mail_article_category_from_symbols(
        [
            {"symbol": "VCB", "impact_score": 70, "relevance_score": 80, "confidence": 90},
            {"symbol": "JPM", "impact_score": 40, "relevance_score": 70, "confidence": 80},
        ],
        {
            "VCB": {"category": "Ngân hàng", "category_slug": "ngan_hang", "is_vn": "true"},
            "JPM": {"category": "World", "category_slug": "world", "is_vn": "false"},
        },
    ) == ("Ngân hàng", "ngan_hang")
    assert svc._news_mail_article_category_from_symbols(
        [
            {"symbol": "VCB", "impact_score": 20, "relevance_score": 80, "confidence": 90},
            {"symbol": "JPM", "impact_score": 95, "relevance_score": 70, "confidence": 80},
        ],
        {
            "VCB": {"category": "Ngân hàng", "category_slug": "ngan_hang", "is_vn": "true"},
            "JPM": {"category": "World", "category_slug": "world", "is_vn": "false"},
        },
    ) == ("World", "world")


def test_ensure_news_mail_tables_uses_advisory_lock_and_runs_once(monkeypatch):
    import app.services.news_mail_service as svc

    calls: list[str] = []

    class FakeCursor:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def execute(self, query, params=None):
            calls.append(str(query))

    class FakeConnection:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def cursor(self):
            return FakeCursor()

        def commit(self):
            calls.append("COMMIT")

    monkeypatch.setattr(svc, "_news_mail_tables_ready", False)
    monkeypatch.setattr(svc, "connect", lambda *_args, **_kwargs: FakeConnection())

    svc.ensure_news_mail_tables()
    svc.ensure_news_mail_tables()

    assert sum("pg_advisory_xact_lock" in query for query in calls) == 1
    assert sum("CREATE TABLE IF NOT EXISTS news_mail_runs" in query for query in calls) == 1
    assert calls.count("COMMIT") == 1


def test_refresh_news_mail_skips_existing_urls_and_continues(monkeypatch):
    import app.services.news_mail_service as svc

    run_id = uuid4()
    message_id = uuid4()
    calls: list[str] = []

    monkeypatch.setattr(svc, "ensure_news_mail_tables", lambda: None)
    monkeypatch.setattr(svc, "_local_today", lambda: date(2026, 6, 1))
    monkeypatch.setattr(
        svc,
        "_create_or_reset_run",
        lambda **kwargs: {"id": run_id, "status": "prepared"},
    )
    monkeypatch.setattr(
        svc._gmail,
        "fetch_today_message_documents",
        lambda **kwargs: [{"html": "", "text": "https://example.vn/a https://example.vn/b https://example.vn/c"}],
    )
    monkeypatch.setattr(svc, "_insert_message", lambda run_id, message: message_id)

    def fake_insert_article_if_new(run_id, message_id, link):
        url = link["url"]
        calls.append(url)
        if url == "https://example.vn/a":
            return {"inserted": False, "article_id": uuid4(), "skip_reason": "duplicate_url"}
        return {"inserted": True, "article_id": uuid4(), "skip_reason": None}

    monkeypatch.setattr(svc, "_insert_article_if_new", fake_insert_article_if_new)
    monkeypatch.setattr(svc, "_mark_run", lambda *args, **kwargs: None)
    monkeypatch.setattr(svc, "_run_summary", lambda run_id: {"id": str(run_id), "status": "prepared"})
    monkeypatch.setattr(svc, "_articles_for_codex", lambda run_id, include_failed: [])

    result = svc.refresh_news_mail_from_gmail(article_fetch_limit=2)

    assert result["new_article_count"] == 2
    assert result["duplicate_skipped_count"] == 1
    assert calls == ["https://example.vn/a", "https://example.vn/b", "https://example.vn/c"]


def test_refresh_news_mail_workflow_runs_analysis_after_scan(monkeypatch):
    import app.services.news_mail_service as svc

    run_id = uuid4()
    article_id = uuid4()
    applied: list[dict[str, object]] = []
    status_marks: list[dict[str, object]] = []

    monkeypatch.setattr(
        svc,
        "refresh_news_mail_from_gmail",
        lambda **kwargs: {
            "success": True,
            "run": {"id": str(run_id), "status": "prepared"},
            "articles": [],
            "new_article_count": 1,
            "duplicate_skipped_count": 0,
            "empty_skipped_count": 0,
            "links_scanned": 1,
        },
    )

    def fake_articles_for_codex(run_id_arg, *, include_failed, only_missing_analysis=False):
        article = {
            "article_id": str(article_id),
            "section_title": "CafeF",
            "title": "CafeF news",
            "url": "https://cafef.vn/news.chn",
            "source_host": "cafef.vn",
            "fetch_status": "fetched",
            "fetch_error": "",
            "article_text": "FPT cong bo hop dong moi.",
        }
        return [article]

    monkeypatch.setattr(svc, "_articles_for_codex", fake_articles_for_codex)
    monkeypatch.setattr(
        svc,
        "_run_codex_cli_json",
        lambda *args, **kwargs: json.dumps(
            {
                "articles": [
                    {
                        "article_id": str(article_id),
                        "summary": "FPT co tin tich cuc.",
                        "key_points": ["Hop dong moi ho tro doanh thu."],
                        "sector_tags": ["cong nghe"],
                        "market_tags": ["co phieu rieng le"],
                        "data_gaps": [],
                        "symbols": [
                            {
                                "symbol": "FPT",
                                "company_name": "FPT",
                                "relevance_score": 95,
                                "sentiment_label": "positive",
                                "sentiment_score": 70,
                                "impact_score": 75,
                                "impact_horizon": "T+3",
                                "confidence": 85,
                                "rationale": "Tin ho tro ky vong tang truong.",
                            }
                        ],
                    }
                ]
            }
        ),
    )

    def fake_apply_codex_results(**kwargs):
        applied.append(kwargs)
        return {"success": True, "updated_articles": 1, "written_impacts": 1}

    monkeypatch.setattr(svc, "apply_codex_results", fake_apply_codex_results)
    monkeypatch.setattr(
        svc,
        "_mark_articles_analysis_status",
        lambda **kwargs: status_marks.append(kwargs) or len(kwargs.get("article_ids") or []),
    )
    monkeypatch.setattr(svc, "_run_summary", lambda run_id_arg: {"id": str(run_id_arg), "status": "completed"})

    result = svc.refresh_news_mail_workflow_from_gmail(article_fetch_limit=2)

    assert result["workflow"]["analysis_status"] == "completed"
    assert result["workflow"]["updated_articles"] == 1
    assert applied
    assert applied[0]["run_id"] == str(run_id)
    assert applied[0]["final_batch"] is True
    assert status_marks[0]["status"] == "codex_running"


def test_news_mail_workflow_marks_failed_article_batch(monkeypatch):
    import app.services.news_mail_service as svc

    run_id = uuid4()
    article_id = uuid4()
    marks: list[dict[str, object]] = []

    monkeypatch.setattr(svc, "refresh_sentiment_return_research", lambda **kwargs: {})
    monkeypatch.setattr(
        svc,
        "_mark_articles_analysis_status",
        lambda **kwargs: marks.append(kwargs) or len(kwargs.get("article_ids") or []),
    )
    monkeypatch.setattr(svc, "_run_codex_cli_json", lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("codex boom")))

    try:
        svc._run_news_mail_analysis_workflow(
            run_id=run_id,
            articles=[
                {
                    "article_id": str(article_id),
                    "title": "Bad batch",
                    "url": "https://example.vn/news",
                    "fetch_status": "fetched",
                    "article_text": "FPT co tin moi.",
                }
            ],
            source="unit_test",
        )
    except RuntimeError:
        pass

    assert marks[0]["status"] == "codex_running"
    assert marks[1]["status"] == "failed"
    assert marks[1]["article_ids"] == [str(article_id)]


def test_news_mail_workflow_skips_completed_articles_inside_batch(monkeypatch):
    import app.services.news_mail_service as svc

    run_id = uuid4()
    done_id = uuid4()
    pending_id = uuid4()
    marked: list[dict[str, object]] = []
    applied: list[list[str]] = []

    monkeypatch.setattr(svc, "refresh_sentiment_return_research", lambda **kwargs: {})
    monkeypatch.setattr(
        svc,
        "_mark_articles_analysis_status",
        lambda **kwargs: marked.append(kwargs) or len(kwargs.get("article_ids") or []),
    )
    monkeypatch.setattr(
        svc,
        "_run_codex_cli_json",
        lambda prompt, *, batch_number: json.dumps(
            {
                "articles": [
                    {
                        "article_id": str(pending_id),
                        "summary": "Pending article done.",
                        "key_points": [],
                        "sector_tags": [],
                        "market_tags": [],
                        "data_gaps": [],
                        "symbols": [],
                    }
                ]
            }
        ),
    )

    def fake_apply_codex_results(**kwargs):
        applied.append([row["article_id"] for row in kwargs["articles"]])
        return {"success": True, "updated_articles": len(kwargs["articles"]), "written_impacts": 0}

    monkeypatch.setattr(svc, "apply_codex_results", fake_apply_codex_results)

    result = svc._run_news_mail_analysis_workflow(
        run_id=run_id,
        articles=[
            {"article_id": str(done_id), "codex_analysis_status": "completed", "title": "Done"},
            {"article_id": str(pending_id), "codex_analysis_status": "pending", "title": "Pending"},
        ],
        source="unit_test",
    )

    assert result["updated_articles"] == 1
    assert marked[0]["article_ids"] == [str(pending_id)]
    assert applied == [[str(pending_id)]]
