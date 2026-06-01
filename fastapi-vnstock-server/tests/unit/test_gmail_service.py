from __future__ import annotations


class _Executable:
    def __init__(self, payload):
        self._payload = payload

    def execute(self):
        return self._payload


class _FakeMessages:
    def list(self, **kwargs):
        return _Executable({"messages": [{"id": "gmail-1"}]})

    def get(self, **kwargs):
        return _Executable(
            {
                "threadId": "thread-1",
                "internalDate": "1779987600000",
                "payload": {
                    "headers": [{"name": "Subject", "value": "Daily market news"}],
                },
            }
        )


class _FakeUsers:
    def messages(self):
        return _FakeMessages()


class _FakeGmailApi:
    def users(self):
        return _FakeUsers()


def test_fetch_today_message_documents_returns_full_text_and_html(monkeypatch):
    import app.services.gmail_service as svc

    url = "https://cafef.vn/loat-doanh-nghiep-duoc-bi-xu-phat-muc-cao-nhat-310-trieu-dong-188260531090104584.chn"
    long_text = ("x" * 12000) + url
    long_html = ("<p>" + ("y" * 250000) + f'</p><a href="{url}">CafeF article</a>')
    gmail = svc.GmailFetchService()

    monkeypatch.setattr(svc, "build", lambda *args, **kwargs: _FakeGmailApi())
    monkeypatch.setattr(gmail, "_build_creds", lambda: object())
    monkeypatch.setattr(gmail, "_build_today_query", lambda query: query)
    monkeypatch.setattr(
        gmail,
        "_collect_message_text",
        lambda service, user_id, gmail_message_id, payload: long_text,
    )
    monkeypatch.setattr(gmail, "_collect_message_html", lambda payload: long_html)

    rows = gmail.fetch_today_message_documents(query="Tin tuc chung khoan", max_results=1)

    assert rows[0]["text"] == long_text
    assert rows[0]["html"] == long_html
    assert rows[0]["text"].endswith(url)
    assert rows[0]["html"].endswith(f'</a>')

    text_rows = gmail.fetch_today_message_texts(query="Tin tuc chung khoan", max_results=1)

    assert text_rows[0]["text"] == long_text
