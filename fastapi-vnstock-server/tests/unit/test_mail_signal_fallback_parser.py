from __future__ import annotations


def test_fallback_mail_signal_parser_extracts_only_clear_buy_rows():
    from app.services.mail_signal_scheduler_service import _fallback_parse_mail_signal_items

    mail_rows = [
        {
            "subject": "Screener 3 san",
            "gmail_message_id": "msg-1",
            "text": """
| Ma | Loai | Diem mua | TP1 | TP2 | SL | R/R | Khuyen nghi |
|-----|------|---------|-----|-----|----|-----|-------------|
| **BIG** | Breakout | 7.0 | 10.4 (+49%) | 11.6 (+66%) | 6.8 (-3%) | 1:17.0 | MUA - R/R tot |
| **GEE** | Can cung | 114.2 | 179.7 (+57%) | 201.0 (+76%) | 100.9 (-12%) | 1:4.92 | MUA - R/R tot |
| **POW** | Breakout | 14.3 | 16.9 (+18%) | 18.9 (+32%) | 12.8 (-10%) | 1:1.73 | THEO DOI - R/R chap nhan |
| **VHG** | Can cung | 1.6 | 1.6 (+0%) | 1.8 (+12%) | 1.5 (-6%) | 1:0.0 | TRANH - R/R thap |
""",
        }
    ]

    items = _fallback_parse_mail_signal_items(mail_rows)

    assert [row["symbol"] for row in items] == ["BIG", "GEE"]
    assert items[0]["entry"] == 7000.0
    assert items[0]["take_profit"] == 10400.0
    assert items[0]["stop_loss"] == 6800.0
    assert items[1]["entry"] == 114200.0
