"""Check the exported source snapshot without executing Databricks code."""
import ast
import hashlib
import json
from pathlib import Path
import re

ROOT = Path(__file__).resolve().parents[1]
EXPECTED_PUBLIC = {
    'workspace.kkbox_silver.members',
    'workspace.kkbox_silver.subscription_transactions',
    'workspace.kkbox_silver.daily_listening_activity',
    *{'workspace.kkbox_gold.' + name for name in (
        'cohorts', 'completion_mix', 'engagement', 'listening_quality',
        'registration_channels', 'signups', 'source_coverage', 'subscriptions')},
}
EXPECTED_PRIVATE = {'workspace.kkbox_gold.' + name for name in (
    '_registration_monthly', '_listening_monthly', '_subscription_raw_events',
    '_subscription_events', '_subscription_intervals', '_subscription_episodes')}

public, private = set(), set()
files = sorted(ROOT.rglob('*.py'))
for path in files:
    if '.git' in path.parts:
        continue
    tree = ast.parse(path.read_text(), filename=str(path.relative_to(ROOT)))
    if path.parent != ROOT / 'pipeline/transformations':
        continue
    for node in ast.walk(tree):
        if not isinstance(node, ast.FunctionDef):
            continue
        for decorator in node.decorator_list:
            if not (isinstance(decorator, ast.Call)
                    and isinstance(decorator.func, ast.Attribute)
                    and decorator.func.attr == 'materialized_view'):
                continue
            options = {kw.arg: ast.literal_eval(kw.value)
                       for kw in decorator.keywords
                       if kw.arg in {'name', 'private'}}
            (private if options.get('private') else public).add(options['name'])

assert public == EXPECTED_PUBLIC, f'Unexpected public dataset inventory: {public}'
assert private == EXPECTED_PRIVATE, f'Unexpected private dataset inventory: {private}'

manifest = json.loads((ROOT / 'docs/source_manifest.json').read_text())
assert len(manifest) == 12, 'Expected twelve exported source files'
for entry in manifest:
    actual = hashlib.sha256((ROOT / entry['destination']).read_bytes()).hexdigest()
    assert actual == entry['sha256'], f"Source changed: {entry['destination']}"

credential_patterns = (
    r'KGAT_[A-Za-z0-9]{12,}', r'gh[pousr]_[A-Za-z0-9]{20,}',
    r'github_pat_[A-Za-z0-9_]{20,}', r'AKIA[A-Z0-9]{16}',
    r'-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----',
)
for path in ROOT.rglob('*'):
    if not path.is_file() or '.git' in path.parts:
        continue
    content = path.read_text()
    assert not any(re.search(pattern, content) for pattern in credential_patterns), (
        f'Possible credential literal in {path.relative_to(ROOT)}')

json.loads((ROOT / 'pipeline/pipeline.example.json').read_text())
print(f'Validated {len(files)} Python files, 12 source hashes, '
      '11 public datasets, 6 private helpers, and credential checks.')
