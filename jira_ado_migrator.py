import os
import sys
import time
import yaml
import json
import logging
import click
from logging.handlers import RotatingFileHandler
from ratelimiter import RateLimiter
from azure.devops.connection import Connection
from msrest.authentication import BasicAuthentication
import requests

# Module logger
glogger = logging.getLogger('jira_ado_migrator')

def setup_logging(level, log_file=None, max_bytes=10485760, backup_count=3):
    for handler in list(glogger.handlers):
        glogger.removeHandler(handler)
    numeric_level = getattr(logging, level.upper(), None)
    glogger.setLevel(numeric_level)
    fmt = logging.Formatter(fmt='%(asctime)s %(levelname)s [%(name)s] %(message)s',
                            datefmt='%Y-%m-%dT%H:%M:%S%z')
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(numeric_level)
    ch.setFormatter(fmt)
    glogger.addHandler(ch)
    if log_file:
        fh = RotatingFileHandler(log_file, maxBytes=max_bytes, backupCount=backup_count)
        fh.setLevel(numeric_level)
        fh.setFormatter(fmt)
        glogger.addHandler(fh)

def load_config(path):
    try:
        with open(path, 'r') as f:
            cfg = yaml.safe_load(f)
    except Exception as e:
        glogger.error(f"Failed to load config file {path}: {e}")
        sys.exit(1)
    if 'user-mapping-file' in cfg:
        user_map = {}
        try:
            with open(cfg['user-mapping-file'], 'r') as uf:
                for line in uf:
                    if '->' in line:
                        jira_user, ado_user = line.strip().split('->')
                        user_map[jira_user.strip()] = ado_user.strip()
            cfg['user-mapping'] = user_map
        except Exception as e:
            glogger.warning(f"Failed to load user-mapping-file: {e}")
    cfg.setdefault('attachments-dir', 'attachments')
    os.makedirs(cfg['attachments-dir'], exist_ok=True)
    return cfg

class JiraClient:
    def __init__(self, base_url, user, token, rate_limit=1):
        self.base_url = base_url.rstrip('/')
        self.session = requests.Session()
        self.session.auth = (user, token)
        self.rate_limiter = RateLimiter(max_calls=rate_limit, period=1)

    def get(self, path, params=None):
        with self.rate_limiter:
            url = f"{self.base_url}/rest/api/2/{path}"
            glogger.debug(f"JIRA GET {url} params={params}")
            resp = self.session.get(url, params=params)
            if resp.status_code == 429:
                glogger.warning("Rate limit hit (429), sleeping 1s and retrying")
                time.sleep(1)
                return self.get(path, params)
            resp.raise_for_status()
            data = resp.json()
            glogger.debug(f"JIRA response keys: {list(data.keys())}")
            return data

    def download_attachment(self, attachment, dest_dir):
        url = attachment.get('content')
        filename = attachment.get('filename')
        filepath = os.path.join(dest_dir, filename)
        with self.rate_limiter:
            glogger.info(f"Downloading attachment {filename}")
            resp = self.session.get(url, stream=True)
            if resp.status_code == 429:
                glogger.warning("Rate limit hit on attachment, sleeping 1s and retrying")
                time.sleep(1)
                return self.download_attachment(attachment, dest_dir)
            resp.raise_for_status()
            with open(filepath, 'wb') as f:
                for chunk in resp.iter_content(chunk_size=8192):
                    f.write(chunk)
        glogger.debug(f"Saved attachment to {filepath}")
        return filepath

def create_azure_client(org_url, pat):
    cred = BasicAuthentication('', pat)
    connection = Connection(base_url=org_url, creds=cred)
    return connection.clients.get_work_item_tracking_client()

def map_title(summary, key, config):
    if config.get('prefix-title', False):
        return f"[{key}] {summary}"
    return summary

def map_user(user_field, config):
    if not user_field:
        return None
    name = user_field.get('name') or user_field.get('displayName')
    return config.get('user-mapping', {}).get(name, name)

def map_rendered(text):
    return text

def map_description(desc):
    if not desc:
        return ''
    return desc

def map_issue(jira_issue, config, jira_client):
    glogger.debug(f"Mapping issue {jira_issue.get('key')}")
    fields = jira_issue.get('fields', {})
    ado = {}
    summary = fields.get('summary', '')
    ado['System.Title'] = map_title(summary, jira_issue.get('key'), config)
    glogger.debug(f"Mapped Title: {ado['System.Title']}")
    description = fields.get('description')
    ado['System.Description'] = map_description(description)
    glogger.debug(f"Mapped Description length: {len(ado['System.Description'])}")
    labels = fields.get('labels', [])
    if labels:
        ado['System.Tags'] = '; '.join(labels)
        glogger.debug(f"Mapped Labels to System.Tags: {ado['System.Tags']}")
    reporter = fields.get('reporter')
    if reporter:
        rep_name = reporter.get('name') or reporter.get('displayName')
        ado['System.CreatedBy'] = rep_name
        glogger.debug(f"Mapped Reporter to System.CreatedBy: {rep_name}")
    for fm in config.get('field-map', {}).get('field', []):
        src, tgt = fm.get('source'), fm.get('target')
        if src in ('summary', 'description', 'labels', 'components', 'Sprint'):
            continue
        raw_val = fields.get(src)
        if raw_val is None:
            continue
        if isinstance(raw_val, dict) and 'name' in raw_val:
            val = raw_val['name']
        else:
            val = raw_val
        if 'mapper' in fm:
            mapper = fm['mapper']
            if mapper == 'MapUser':
                mapped = map_user(val, config)
            elif mapper == 'MapRendered':
                mapped = map_rendered(val)
            else:
                mapped = val
        elif fm.get('mapping') and fm['mapping'].get('values'):
            vm = {v['source']: v['target'] for v in fm['mapping']['values']}
            mapped = vm.get(val, val)
        else:
            mapped = val
        if isinstance(mapped, list):
            mapped = '; '.join(item.get('name', item) for item in mapped)
        ado[tgt] = mapped
        glogger.debug(f"Mapped field {src} to {tgt}: {ado[tgt]}")
    comments = []
    try:
        resp_c = jira_client.get(f"issue/{jira_issue.get('key')}/comment")
        for c in resp_c.get('comments', []):
            author = c.get('author', {}).get('name') or c.get('author', {}).get('displayName')
            ado_author = config.get('user-mapping', {}).get(author)
            if not ado_author:
                ado_author = 'sven.schneider@mhp.com'
                body = f"_Original author: {author}_{c.get('body','')}"
            else:
                body = c.get('body','')
            comments.append({'author': ado_author, 'body': body})
    except Exception as e:
        glogger.warning(f"Failed to fetch comments for {jira_issue.get('key')}: {e}")
    ado['_comments'] = comments
    return ado

def create_work_item(wit_client, project, ado_data):
    glogger.info(f"Creating work item of type {ado_data.get('System.WorkItemType')}")
    # implement patch document building...
    return None

def upload_attachments_and_relations(wit_client, project, wi_id, ado_data):
    glogger.info(f"Uploading attachments and relations to work item {wi_id}")
    # implement attachments & relations...

def upload_attachments_relations_comments(wit_client, project, wi_id, ado_data):
    upload_attachments_and_relations(wit_client, project, wi_id, ado_data)
    for c in ado_data.get('_comments', []):
        try:
            wit_client.add_comment(wi_id, c['body'], project=project)
        except Exception as e:
            glogger.error(f"Failed to add comment to {wi_id}: {e}")

@click.group()
@click.option('--config', '-c', default='config.yaml', help='YAML config path')
@click.option('--log-level', default='INFO', help='Logging level')
@click.option('--log-file', default=None, help='Optional log file path')
@click.pass_context
def cli(ctx, config, log_level, log_file):
    setup_logging(log_level, log_file)
    glogger.info('Starting Jira؟Azure DevOps migration')
    cfg = load_config(config)
    ctx.obj = cfg
    global jira_client, wit_client
    jira_client = JiraClient(cfg['jira']['url'], cfg['jira']['user'], cfg['jira']['token'], rate_limit=cfg.get('rate-limit',1))
    wit_client = create_azure_client(cfg['azure']['org_url'], cfg['azure']['pat'])

@cli.command(name='export-only')
@click.pass_context
def export_only(ctx):
    """Export issues from Jira to local JSON file with journaling"""
    cfg = ctx.obj
    file_path = cfg.get('export_file', 'jira_issues.json')

    if os.path.exists(file_path):
        try:
            with open(file_path, 'r') as f:
                issues = json.load(f)
            start = len(issues)
            glogger.info(f"Resuming export from offset {start}, {start} issues already fetched")
        except Exception as e:
            glogger.warning(f"Failed to load existing export file, starting fresh: {e}")
            issues = []
            start = 0
    else:
        issues = []
        start = 0

    batch = cfg.get('batch_size', 50)
    fields = cfg['jira']['fields']
    jql = cfg['jira']['jql']
    initial_count = start

    while True:
        resp = jira_client.get('search', params={'jql': jql, 'startAt': start, 'maxResults': batch, 'fields': fields})
        chunk = resp.get('issues', [])
        if not chunk:
            glogger.info("No new issues found, stopping export")
            break
        issues.extend(chunk)
        start += len(chunk)
        total = resp.get('total', 0)
        glogger.info(f"Fetched {start}/{total} issues")
        if start >= total:
            break

    try:
        with open(file_path, 'w') as f:
            json.dump(issues, f, indent=2)
        new_count = len(issues) - initial_count
        glogger.info(f"Export complete: {new_count} new issues, total {len(issues)} saved to {file_path}")
    except Exception as e:
        glogger.error(f"Failed to write export file: {e}")

@cli.command(name='import-only')
@click.pass_context
def import_only(ctx):
    """Import issues from local JSON into Azure DevOps with journaling and resume support"""
    cfg = ctx.obj
    export_file = cfg.get('export_file', 'jira_issues.json')
    journal_file = cfg.get('import_journal_file', 'import_journal.json')

    try:
        with open(export_file, 'r') as f:
            issues = json.load(f)
    except Exception as e:
        glogger.error(f"Failed to read export file {export_file}: {e}")
        sys.exit(1)

    if os.path.exists(journal_file):
        try:
            with open(journal_file, 'r') as jf:
                journal = json.load(jf)
            glogger.info(f"Loaded import journal with {len(journal)} entries")
        except Exception as e:
            glogger.warning(f"Failed to load import journal, starting fresh: {e}")
            journal = {}
    else:
        journal = {}

    success_count = 0
    failure_count = 0
    total_to_import = len(issues)

    for issue in issues:
        key = issue.get('key')
        if key in journal:
            glogger.debug(f"Skipping already imported issue {key}")
            continue
        try:
            ado_data = map_issue(issue, cfg, jira_client)
            wi_id = create_work_item(wit_client, cfg['azure']['project'], ado_data)
            upload_attachments_relations_comments(wit_client, cfg['azure']['project'], wi_id, ado_data)
            journal[key] = wi_id
            success_count += 1
            glogger.info(f"Imported {key} as work item {wi_id}")
        except Exception as e:
            failure_count += 1
            glogger.error(f"Failed to import {key}: {e}")
            continue

    try:
        with open(journal_file, 'w') as jf:
            json.dump(journal, jf, indent=2)
        glogger.info(f"Import journal saved with {len(journal)} entries to {journal_file}")
    except Exception as e:
        glogger.error(f"Failed to write import journal: {e}")

    glogger.info(f"Import summary: {success_count} succeeded, {failure_count} failed, {total_to_import - len(journal)} skipped (already imported)")
    glogger.info(f"Total issues processed: {total_to_import}")

if __name__=='__main__':
    try:
        cli()
    except Exception as e:
        glogger.exception(f"Unhandled error: {e}")
        sys.exit(1)
