"""Vercel Serverless Function — fetch AudioFeel data from Google Sheets."""

import json
import time
import base64
import tempfile
import subprocess
import os
import urllib.request
import urllib.parse
from datetime import datetime, timedelta
from collections import defaultdict
from http.server import BaseHTTPRequestHandler

SHEET_ID = '1AovNcks6gL8EvtP30NtYL4nT9RsVefUG_QeAvYqQkpg'
SCOPES = 'https://www.googleapis.com/auth/spreadsheets.readonly'

RANGES = [
    'Raw!A1:I600',
    'Raw_Platforms!A1:S600',
]


def get_sa_key():
    """Load SA key from GOOGLE_SA_KEY env var (full JSON string)."""
    raw = os.environ.get('GOOGLE_SA_KEY', '')
    if not raw:
        raise RuntimeError('GOOGLE_SA_KEY env var not set')
    return json.loads(raw)


def b64url(data):
    if isinstance(data, str):
        data = data.encode('utf-8')
    return base64.urlsafe_b64encode(data).rstrip(b'=').decode('ascii')


def get_token():
    sa = get_sa_key()
    now = int(time.time())
    header = json.dumps({'alg': 'RS256', 'typ': 'JWT'})
    payload = json.dumps({
        'iss': sa['client_email'],
        'scope': SCOPES,
        'aud': 'https://oauth2.googleapis.com/token',
        'iat': now,
        'exp': now + 3600,
    })
    unsigned = f"{b64url(header)}.{b64url(payload)}"
    with tempfile.NamedTemporaryFile(mode='w', suffix='.pem', delete=False) as f:
        f.write(sa['private_key'])
        key_path = f.name
    try:
        result = subprocess.run(
            ['openssl', 'dgst', '-sha256', '-sign', key_path],
            input=unsigned.encode('utf-8'),
            capture_output=True
        )
        if result.returncode != 0:
            raise RuntimeError(f"openssl error: {result.stderr.decode()}")
    finally:
        os.unlink(key_path)
    jwt_token = f"{unsigned}.{b64url(result.stdout)}"
    data = urllib.parse.urlencode({
        'grant_type': 'urn:ietf:params:oauth:grant-type:jwt-bearer',
        'assertion': jwt_token,
    }).encode()
    req = urllib.request.Request('https://oauth2.googleapis.com/token', data=data)
    with urllib.request.urlopen(req) as resp:
        return json.loads(resp.read())['access_token']


def fetch_ranges(token, ranges):
    params = '&'.join(f'ranges={urllib.parse.quote(r)}' for r in ranges)
    url = f'https://sheets.googleapis.com/v4/spreadsheets/{SHEET_ID}/values:batchGet?{params}'
    req = urllib.request.Request(url)
    req.add_header('Authorization', f'Bearer {token}')
    with urllib.request.urlopen(req) as resp:
        return json.loads(resp.read())


def parse_date_dmy(s):
    """Parse DD/MM/YYYY or Excel serial date number to YYYY-MM-DD."""
    s = s.strip()
    if not s:
        return None
    try:
        serial = int(s)
        if 40000 < serial < 60000:
            base = datetime(1899, 12, 30)
            d = base + timedelta(days=serial)
            return d.strftime('%Y-%m-%d')
    except ValueError:
        pass
    parts = s.split('/')
    if len(parts) != 3:
        return None
    d, m, y = parts
    if len(y) == 2:
        y = '20' + y
    try:
        return f"{int(y):04d}-{int(m):02d}-{int(d):02d}"
    except ValueError:
        return None


def parse_date_mdy(s):
    """Parse M/D/YYYY or Excel serial date number to YYYY-MM-DD."""
    s = s.strip()
    if not s:
        return None
    try:
        serial = int(s)
        if 40000 < serial < 60000:
            base = datetime(1899, 12, 30)
            d = base + timedelta(days=serial)
            return d.strftime('%Y-%m-%d')
    except ValueError:
        pass
    try:
        serial = float(s)
        if 40000 < serial < 60000:
            base = datetime(1899, 12, 30)
            d = base + timedelta(days=int(serial))
            return d.strftime('%Y-%m-%d')
    except ValueError:
        pass
    parts = s.split('/')
    if len(parts) != 3:
        return None
    m, d, y = parts
    if len(y) == 2:
        y = '20' + y
    try:
        return f"{int(y):04d}-{int(m):02d}-{int(d):02d}"
    except ValueError:
        return None


def safe_float(val, default=0.0):
    if not val or val in ('--', '', '#DIV/0!', '#N/A', '#ERROR!'):
        return default
    try:
        return float(str(val).replace(',', '').replace('\u20aa', '').replace('%', '').strip())
    except (ValueError, TypeError):
        return default


def process_data(value_ranges):
    raw_rows = value_ranges[0].get('values', [])
    platform_rows = value_ranges[1].get('values', [])

    # --- Parse Raw (Shopify) data ---
    shopify = {}
    for i, row in enumerate(raw_rows):
        if i == 0:
            continue
        if len(row) < 1 or row[0] in ('--', ''):
            continue
        date_str = parse_date_dmy(row[0])
        if not date_str:
            continue
        shopify[date_str] = {
            'revenue': safe_float(row[1] if len(row) > 1 else 0),
            'orders': int(safe_float(row[2] if len(row) > 2 else 0)),
            'new_revenue': safe_float(row[3] if len(row) > 3 else 0),
            'new_customers': int(safe_float(row[4] if len(row) > 4 else 0)),
            'discounts': safe_float(row[5] if len(row) > 5 else 0),
            'refunds': safe_float(row[6] if len(row) > 6 else 0),
        }

    # --- Parse Raw_Platforms (Meta + Google) data ---
    meta_data = {}
    google_data = {}

    for i, row in enumerate(platform_rows):
        if i == 0:
            continue
        if len(row) < 1 or row[0] in ('--', ''):
            continue

        # Meta columns (0-8)
        meta_date = parse_date_mdy(row[0]) if len(row) > 0 and row[0] else None
        if meta_date:
            meta_data[meta_date] = {
                'spend': safe_float(row[1] if len(row) > 1 else 0),
                'conversions': safe_float(row[2] if len(row) > 2 else 0),
                'cpa': safe_float(row[3] if len(row) > 3 else 0),
                'value': safe_float(row[4] if len(row) > 4 else 0),
                'roas': safe_float(row[5] if len(row) > 5 else 0),
                'cpm': safe_float(row[6] if len(row) > 6 else 0),
                'ctr': safe_float(row[7] if len(row) > 7 else 0),
                'cpc': safe_float(row[8] if len(row) > 8 else 0),
            }

        # Google columns (10-17)
        google_date = parse_date_mdy(row[10]) if len(row) > 10 and row[10] else None
        if google_date:
            google_data[google_date] = {
                'spend': safe_float(row[11] if len(row) > 11 else 0),
                'conversions': safe_float(row[12] if len(row) > 12 else 0),
                'cpa': safe_float(row[13] if len(row) > 13 else 0),
                'value': safe_float(row[14] if len(row) > 14 else 0),
                'roas': safe_float(row[15] if len(row) > 15 else 0),
                'cpc': safe_float(row[16] if len(row) > 16 else 0),
                'ctr': safe_float(row[17] if len(row) > 17 else 0),
            }

    # --- Merge: only dates that have platform spend data ---
    all_platform_dates = set(meta_data.keys()) | set(google_data.keys())

    daily = []
    for date_str in sorted(all_platform_dates):
        meta = meta_data.get(date_str, {})
        google = google_data.get(date_str, {})
        shop = shopify.get(date_str, {})

        meta_spend = meta.get('spend', 0)
        google_spend = google.get('spend', 0)
        total_spend = meta_spend + google_spend

        if total_spend < 1:
            continue

        revenue = shop.get('revenue', 0)
        orders = shop.get('orders', 0)
        new_revenue = shop.get('new_revenue', 0)
        new_customers = shop.get('new_customers', 0)

        mer = revenue / total_spend if total_spend > 0 else 0
        cpa = total_spend / orders if orders > 0 else 0
        nc_roas = new_revenue / total_spend if total_spend > 0 else 0
        cac = total_spend / new_customers if new_customers > 0 else 0

        daily.append({
            'date': date_str,
            'revenue': round(revenue, 2),
            'orders': orders,
            'new_revenue': round(new_revenue, 2),
            'new_customers': new_customers,
            'returning_revenue': round(revenue - new_revenue, 2),
            'returning_orders': orders - new_customers,
            'total_spend': round(total_spend, 2),
            'meta_spend': round(meta_spend, 2),
            'google_spend': round(google_spend, 2),
            'tiktok_spend': 0,
            'meta': {
                'spend': round(meta_spend, 2),
                'conversions': meta.get('conversions', 0),
                'cpa': round(meta.get('cpa', 0), 2),
                'value': round(meta.get('value', 0), 2),
                'roas': round(meta.get('roas', 0), 2),
                'cpm': round(meta.get('cpm', 0), 2),
                'ctr': round(meta.get('ctr', 0), 2),
                'cpc': round(meta.get('cpc', 0), 2),
            },
            'google': {
                'spend': round(google_spend, 2),
                'conversions': google.get('conversions', 0),
                'cpa': round(google.get('cpa', 0), 2),
                'value': round(google.get('value', 0), 2),
                'roas': round(google.get('roas', 0), 2),
                'cpc': round(google.get('cpc', 0), 2),
                'ctr': round(google.get('ctr', 0), 2),
            },
            'tiktok': {},
            'mer': round(mer, 4),
            'cpa': round(cpa, 2),
            'nc_roas': round(nc_roas, 4),
            'cac': round(cac, 2),
        })

    # --- Weekly aggregation (Mon-Sun) ---
    weekly = []
    if daily:
        week_groups = defaultdict(list)
        for d in daily:
            dt = datetime.strptime(d['date'], '%Y-%m-%d')
            monday = dt - timedelta(days=dt.weekday())
            week_groups[monday.strftime('%Y-%m-%d')].append(d)

        for week_start_str in sorted(week_groups.keys()):
            days = week_groups[week_start_str]
            ws = datetime.strptime(week_start_str, '%Y-%m-%d')
            we = ws + timedelta(days=6)
            revenue = sum(d['revenue'] for d in days)
            orders = sum(d['orders'] for d in days)
            new_revenue = sum(d['new_revenue'] for d in days)
            new_customers = sum(d['new_customers'] for d in days)
            total_spend = sum(d['total_spend'] for d in days)

            weekly.append({
                'week_start': week_start_str,
                'week_end': we.strftime('%Y-%m-%d'),
                'revenue': round(revenue, 2),
                'orders': orders,
                'new_revenue': round(new_revenue, 2),
                'new_customers': new_customers,
                'returning_revenue': round(revenue - new_revenue, 2),
                'returning_orders': orders - new_customers,
                'total_spend': round(total_spend, 2),
                'meta_spend': round(sum(d['meta_spend'] for d in days), 2),
                'google_spend': round(sum(d['google_spend'] for d in days), 2),
                'tiktok_spend': 0,
                'mer': round(revenue / total_spend, 4) if total_spend > 0 else 0,
                'cpa': round(total_spend / orders, 2) if orders > 0 else 0,
                'nc_roas': round(new_revenue / total_spend, 4) if total_spend > 0 else 0,
                'cac': round(total_spend / new_customers, 2) if new_customers > 0 else 0,
            })

    # --- Monthly aggregation ---
    monthly = []
    if daily:
        month_groups = defaultdict(list)
        for d in daily:
            month_groups[d['date'][:7]].append(d)

        for month_key in sorted(month_groups.keys()):
            days = month_groups[month_key]
            revenue = sum(d['revenue'] for d in days)
            orders = sum(d['orders'] for d in days)
            new_revenue = sum(d['new_revenue'] for d in days)
            new_customers = sum(d['new_customers'] for d in days)
            total_spend = sum(d['total_spend'] for d in days)

            monthly.append({
                'month': month_key,
                'revenue': round(revenue, 2),
                'orders': orders,
                'new_revenue': round(new_revenue, 2),
                'new_customers': new_customers,
                'returning_revenue': round(revenue - new_revenue, 2),
                'returning_orders': orders - new_customers,
                'total_spend': round(total_spend, 2),
                'meta_spend': round(sum(d['meta_spend'] for d in days), 2),
                'google_spend': round(sum(d['google_spend'] for d in days), 2),
                'tiktok_spend': 0,
                'mer': round(revenue / total_spend, 4) if total_spend > 0 else 0,
                'cpa': round(total_spend / orders, 2) if orders > 0 else 0,
                'nc_roas': round(new_revenue / total_spend, 4) if total_spend > 0 else 0,
                'cac': round(total_spend / new_customers, 2) if new_customers > 0 else 0,
            })

    return {
        'daily': daily,
        'weekly': weekly,
        'monthly': monthly,
        'generated': datetime.utcnow().strftime('%d/%m/%Y %H:%M UTC'),
    }


class handler(BaseHTTPRequestHandler):
    def do_GET(self):
        try:
            token = get_token()
            result = fetch_ranges(token, RANGES)
            value_ranges = result.get('valueRanges', [])
            data = process_data(value_ranges)

            body = json.dumps(data, ensure_ascii=False).encode('utf-8')
            self.send_response(200)
            self.send_header('Content-Type', 'application/json; charset=utf-8')
            self.send_header('Cache-Control', 's-maxage=300, stale-while-revalidate=60')
            self.end_headers()
            self.wfile.write(body)
        except Exception as e:
            error = json.dumps({'error': str(e)}).encode('utf-8')
            self.send_response(500)
            self.send_header('Content-Type', 'application/json; charset=utf-8')
            self.end_headers()
            self.wfile.write(error)
