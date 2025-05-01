import os
import json
import pytest
from click.testing import CliRunner
import jira_ado_migrator as mig

def test_map_title_with_prefix():
    cfg = {'prefix-title': True}
    title = mig.map_title('My summary', 'PROJ-123', cfg)
    assert title == '[PROJ-123] My summary'

def test_map_title_without_prefix():
    cfg = {'prefix-title': False}
    title = mig.map_title('My summary', 'PROJ-123', cfg)
    assert title == 'My summary'

@pytest.mark.parametrize('desc, expected', [
    (None, ''),
    ('<p>Test</p>', '<p>Test</p>'),
])
def test_map_description(desc, expected):
    assert mig.map_description(desc) == expected

class DummyJiraClient:
    def __init__(self): pass
    def get(self, path, params=None):
        start = params.get('startAt', 0)
        total = 3
        per_page = params.get('maxResults', 2)
        issues = []
        for i in range(start, min(start+per_page, total)):
            issues.append({'key': f'ISSUE-{i}', 'fields': {'summary': 'S', 'description': 'D', 'labels': [], 'reporter': {'name': 'user1'}}})
        return {'issues': issues, 'total': total}

def test_export_only_journaling(tmp_path, monkeypatch):
    cfg = {
        'jira': {'url': 'http://fake', 'user': 'u', 'token': 't', 'jql': 'all', 'fields': ['summary','description','labels','reporter']},
        'batch_size': 2,
        'export_file': str(tmp_path / 'export.json')
    }
    cfg_file = tmp_path / 'config.yaml'
    import yaml
    cfg_file.write_text(yaml.safe_dump(cfg))

    monkeypatch.setattr(mig, 'jira_client', DummyJiraClient())
    runner = CliRunner()
    result1 = runner.invoke(mig.cli, ['export-only', '--config', str(cfg_file)])
    assert result1.exit_code == 0
    data1 = json.loads((tmp_path / 'export.json').read_text())
    assert len(data1) == 2

    result2 = runner.invoke(mig.cli, ['export-only', '--config', str(cfg_file)])
    assert result2.exit_code == 0
    data2 = json.loads((tmp_path / 'export.json').read_text())
    assert len(data2) == 3

class DummyWIClient:
    def __init__(self): self.comments = []
    def create_work_item(self, document, project, type): return type, 1
    def add_comment(self, wi_id, body, project=None): self.comments.append((wi_id, body))

def test_import_journaling(tmp_path, monkeypatch):
    issues = [{'key': 'A', 'fields': {}}, {'key':'B','fields':{}}]
    export_file = tmp_path / 'export.json'
    export_file.write_text(json.dumps(issues))
    cfg = {'export_file': str(export_file), 'import_journal_file': str(tmp_path/'journal.json'), 'azure':{'project':'P'}, 'jira':{}, 'field-map':{}}
    cfg_file = tmp_path / 'config.yaml'
    import yaml
    cfg_file.write_text(yaml.safe_dump(cfg))

    monkeypatch.setattr(mig, 'jira_client', DummyJiraClient())
    wi_client = DummyWIClient()
    monkeypatch.setattr(mig, 'wit_client', wi_client)
    monkeypatch.setattr(mig, 'create_work_item', lambda wc, p, a: 42)
    monkeypatch.setattr(mig, 'upload_attachments_relations_comments', lambda wc, p, id, a: None)
    runner = CliRunner()
    result = runner.invoke(mig.cli, ['import-only', '--config', str(cfg_file)])
    assert result.exit_code == 0
    journal = json.loads((tmp_path/'journal.json').read_text())
    assert journal == {'A': 42, 'B': 42}

def test_jira_client_rate_limit(monkeypatch):
    import time as _time
    import requests as _requests

    calls = []
    class DummyResp:
        def __init__(self, status_code, json_data=None):
            self.status_code = status_code
            self._json = json_data or {}
        def json(self):
            return self._json
        def raise_for_status(self):
            if self.status_code >= 400:
                raise _requests.HTTPError(f"HTTP {self.status_code}")

    def fake_get(url, params=None, **kwargs):
        calls.append(url)
        if len(calls) == 1:
            return DummyResp(429)
        return DummyResp(200, {'ok': True})

    jira = mig.JiraClient('http://fake', 'user', 'token', rate_limit=100)
    monkeypatch.setattr(jira.session, 'get', fake_get)

    start = _time.time()
    result = jira.get('search', params={'jql': 'all', 'startAt': 0, 'maxResults': 1})
    duration = _time.time() - start

    assert result == {'ok': True}
    assert len(calls) == 2
    assert duration >= 1
