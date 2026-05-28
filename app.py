"""
マルエーうちや 商品スキャンアプリ (PWA) v2
JANコードで商品情報・販売実績・受払い・在庫を照会
CHAINS基幹FTPからデータ取込
PostgreSQL永続化対応
"""
import os
import sys
import csv
import hashlib
import tempfile
import shutil
import threading
import ftplib
from datetime import datetime
from flask import Flask, request, jsonify, Response

# ========== アプリ設定 ==========

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATABASE_URL = os.environ.get('DATABASE_URL', '')

app = Flask(__name__)

# ========== DB ユーティリティ ==========

def get_db():
    """PostgreSQL接続を返す"""
    import psycopg2
    import psycopg2.extras
    conn = psycopg2.connect(DATABASE_URL)
    return conn


def query_rows(sql, params=None):
    """SELECT結果をdict listで返す"""
    import psycopg2.extras
    conn = get_db()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute(sql, params or [])
    rows = [dict(r) for r in cur.fetchall()]
    cur.close()
    conn.close()
    return rows


def query_one(sql, params=None):
    """SELECT結果の1行目をdictで返す"""
    import psycopg2.extras
    conn = get_db()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute(sql, params or [])
    row = cur.fetchone()
    cur.close()
    conn.close()
    return dict(row) if row else None


def execute_sql(sql, params=None):
    conn = get_db()
    cur = conn.cursor()
    cur.execute(sql, params or [])
    conn.commit()
    cur.close()
    conn.close()


def ensure_tables():
    """全テーブル作成"""
    conn = get_db()
    cur = conn.cursor()

    # 商品マスタ
    cur.execute("""CREATE TABLE IF NOT EXISTS t_scan_products (
        jan TEXT PRIMARY KEY,
        edp TEXT,
        product_name TEXT,
        product_name_kana TEXT,
        spec TEXT,
        dept_code TEXT,
        dept_l TEXT, dept_m TEXT, dept_s TEXT,
        supplier_code TEXT,
        cost REAL DEFAULT 0,
        sell_price INTEGER DEFAULT 0,
        tax_price INTEGER DEFAULT 0,
        updated_at TEXT
    )""")

    # POS日別
    cur.execute("""CREATE TABLE IF NOT EXISTS t_scan_pos_daily (
        date TEXT,
        store_code TEXT,
        jan TEXT,
        edp TEXT,
        sales_qty INTEGER DEFAULT 0,
        sales_amount INTEGER DEFAULT 0,
        discount_count INTEGER DEFAULT 0,
        discount_amount INTEGER DEFAULT 0,
        cost_amount INTEGER DEFAULT 0,
        PRIMARY KEY (date, store_code, jan)
    )""")

    # 受払い
    cur.execute("""CREATE TABLE IF NOT EXISTS t_scan_ukebarai (
        date TEXT,
        store_code TEXT,
        jan TEXT,
        product_name TEXT,
        sell_price INTEGER DEFAULT 0,
        cost_price REAL DEFAULT 0,
        pos_qty INTEGER DEFAULT 0,
        order_qty INTEGER DEFAULT 0,
        purchase_qty INTEGER DEFAULT 0,
        transfer_in_qty INTEGER DEFAULT 0,
        transfer_out_qty INTEGER DEFAULT 0,
        return_qty INTEGER DEFAULT 0,
        disposal_qty INTEGER DEFAULT 0,
        col14 INTEGER DEFAULT 0,
        col15 INTEGER DEFAULT 0,
        ext_purchase_cost REAL DEFAULT 0,
        ext_purchase_sell REAL DEFAULT 0,
        ext_purchase_qty REAL DEFAULT 0,
        ext_return_cost REAL DEFAULT 0,
        ext_return_sell REAL DEFAULT 0,
        ext_return_qty REAL DEFAULT 0,
        PRIMARY KEY (date, store_code, jan)
    )""")
    # 外部仕入カラム追加（既存テーブル対応）
    for col in ['ext_purchase_cost','ext_purchase_sell','ext_purchase_qty',
                'ext_return_cost','ext_return_sell','ext_return_qty']:
        try:
            cur.execute(f"ALTER TABLE t_scan_ukebarai ADD COLUMN {col} REAL DEFAULT 0")
        except Exception:
            pass  # 既に存在する場合

    # 在庫
    cur.execute("""CREATE TABLE IF NOT EXISTS t_scan_zaiko (
        store_code TEXT,
        store_name TEXT,
        dept_code TEXT,
        edp TEXT,
        jan TEXT,
        product_name TEXT,
        prev_stock INTEGER DEFAULT 0,
        purchase_qty INTEGER DEFAULT 0,
        pos_qty INTEGER DEFAULT 0,
        transfer_in_qty INTEGER DEFAULT 0,
        transfer_out_qty INTEGER DEFAULT 0,
        theory_stock INTEGER DEFAULT 0,
        actual_stock INTEGER DEFAULT 0,
        current_stock INTEGER DEFAULT 0,
        col15 INTEGER DEFAULT 0,
        last_purchase_date TEXT,
        stock_sell_amount REAL DEFAULT 0,
        stock_cost_amount REAL DEFAULT 0,
        updated_at TEXT,
        PRIMARY KEY (store_code, jan)
    )""")

    # 棚割
    cur.execute("""CREATE TABLE IF NOT EXISTS t_scan_tanawari (
        store_code TEXT,
        gondola_no TEXT,
        shelf_no TEXT,
        position REAL,
        jan TEXT,
        product_name TEXT,
        face_count INTEGER DEFAULT 0,
        edp TEXT,
        dept_code TEXT,
        dept_name TEXT,
        sub_dept_code TEXT,
        sub_dept_name TEXT,
        supplier_code TEXT,
        supplier_name TEXT,
        cost REAL DEFAULT 0,
        sell_price INTEGER DEFAULT 0,
        margin_rate REAL DEFAULT 0,
        PRIMARY KEY (store_code, gondola_no, shelf_no, position)
    )""")

    # 時間帯別POS
    cur.execute("""CREATE TABLE IF NOT EXISTS t_scan_jikantai (
        date TEXT,
        time_slot TEXT,
        store_code TEXT,
        dept_code TEXT,
        edp TEXT,
        product_code TEXT,
        jan TEXT,
        qty INTEGER DEFAULT 0,
        amount INTEGER DEFAULT 0,
        PRIMARY KEY (date, time_slot, store_code, jan)
    )""")

    # インデックス
    cur.execute("CREATE INDEX IF NOT EXISTS idx_pos_jan ON t_scan_pos_daily(jan)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_pos_date ON t_scan_pos_daily(date)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_uke_jan ON t_scan_ukebarai(jan)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_uke_date ON t_scan_ukebarai(date)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_zaiko_jan ON t_scan_zaiko(jan)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_tana_jan ON t_scan_tanawari(jan)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_jikan_jan ON t_scan_jikantai(jan)")

    # 同期履歴
    cur.execute("""CREATE TABLE IF NOT EXISTS t_sync_log (
        id SERIAL PRIMARY KEY,
        sync_type TEXT,
        started_at TIMESTAMP DEFAULT NOW(),
        finished_at TIMESTAMP,
        status TEXT,
        detail TEXT
    )""")

    conn.commit()
    cur.close()
    conn.close()


# ========== 店舗マスタ ==========

STORE_NAMES = {
    '5': '泉店', '6': '平面店', '7': '旭南店', '8': '御所野店',
    '9': '酒田北店', '10': '鶴岡店', '11': '本荘石脇店', '12': '茨島店', '13': '鶴岡南店'
}


# ========== ヘルスチェック + 自動同期トリガー ==========

def _check_and_trigger_sync():
    """今日の同期がまだ完了していなければバックグラウンドで同期開始"""
    from datetime import timezone, timedelta
    JST = timezone(timedelta(hours=9))
    now_jst = datetime.now(JST)

    # 指定時刻前なら何もしない
    if now_jst.hour < DAILY_SYNC_HOUR_JST:
        return 'before_schedule'

    # 既に実行中なら何もしない
    if _ftp_sync_status['running']:
        return 'already_running'

    # 今日既に成功済みか確認（last_resultのfinished_atが今日ならスキップ）
    last = _ftp_sync_status.get('last_result')
    if last and last.get('success') and last.get('finished_at'):
        finished = last['finished_at'][:10]  # 'YYYY-MM-DD'
        today_str = now_jst.strftime('%Y-%m-%d')
        if finished == today_str:
            return 'already_done_today'

    # 同期開始
    print(f"[自動同期] /health トリガーで同期開始 ({now_jst.strftime('%Y-%m-%d %H:%M')})", flush=True)
    t = threading.Thread(target=_run_ftp_sync, daemon=True)
    t.start()
    return 'sync_started'


@app.route('/health')
def health_check():
    """外部cronサービスからのkeep-alive用。同期が必要なら自動トリガー。"""
    from datetime import timezone, timedelta
    JST = timezone(timedelta(hours=9))
    now_jst = datetime.now(JST)

    sync_action = 'disabled'
    if DATABASE_URL and FTP_HOST:
        try:
            sync_action = _check_and_trigger_sync()
        except Exception as e:
            sync_action = f'error: {e}'

    return jsonify({
        'status': 'ok',
        'time_jst': now_jst.strftime('%Y-%m-%d %H:%M:%S'),
        'auto_sync': f'毎日 JST {DAILY_SYNC_HOUR_JST}:00',
        'sync_action': sync_action,
    })


# ========== HTMLページ ==========

@app.route('/')
@app.route('/scan')
def scan_page():
    filepath = os.path.join(BASE_DIR, 'scan.html')
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()
    return Response(content, mimetype='text/html; charset=utf-8')


@app.route('/manifest.json')
def manifest_json():
    filepath = os.path.join(BASE_DIR, 'manifest.json')
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()
    return Response(content, mimetype='application/json; charset=utf-8')


@app.route('/sw.js')
def service_worker():
    filepath = os.path.join(BASE_DIR, 'sw.js')
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()
    resp = Response(content, mimetype='application/javascript; charset=utf-8')
    resp.headers['Service-Worker-Allowed'] = '/'
    return resp


# ========== 商品照会API ==========

def _normalize_instore_jan(jan):
    """インストア商品(02始まり)のJAN正規化。
    バーコード: 02XXXXX PPPPP C (13桁) → 価格部分をゼロ埋め
    CHAINSでは 02XXXXX000000 で商品マスタに登録されている場合が多い。
    複数パターンで検索を試みる。"""
    candidates = [jan]  # 元のコードはそのまま先頭
    if len(jan) == 13 and jan.startswith('02'):
        # パターン1: 価格5桁をゼロ埋め（02XXXXX00000C → 02XXXXX000000）
        base7 = jan[:7]  # '02XXXXX'
        candidates.append(base7 + '000000')
        # パターン2: 末尾チェックデジットも含めてゼロ（13桁）
        candidates.append(base7 + '0000000')  # 14桁なので除外
        # パターン3: LIKE検索用（02XXXXX%）
        candidates.append(base7)
    elif len(jan) == 13 and jan.startswith('2'):
        # NONPLUフォーマット: 2XXXXXX PPPPP C
        base7 = jan[:7]
        candidates.append(base7 + '000000')
        candidates.append(base7)
    return candidates


@app.route('/api/scan/product', methods=['GET'])
def scan_product():
    """JANコードで商品情報を検索"""
    jan = request.args.get('jan', '').strip()
    if not jan:
        return jsonify({'error': 'jan parameter required'}), 400

    # まず完全一致
    product = query_one("SELECT * FROM t_scan_products WHERE jan = %s", [jan])

    # インストア商品(02/2始まり)の場合、正規化パターンで検索
    if not product and len(jan) >= 13 and (jan.startswith('02') or jan.startswith('2')):
        candidates = _normalize_instore_jan(jan)
        for cand in candidates[1:]:  # 元コードは既に試した
            product = query_one("SELECT * FROM t_scan_products WHERE jan = %s", [cand])
            if product:
                break
        # それでも見つからなければ前方一致
        if not product:
            prefix = jan[:7] if jan.startswith('02') else jan[:7]
            product = query_one(
                "SELECT * FROM t_scan_products WHERE jan LIKE %s LIMIT 1",
                [prefix + '%'])

    if not product:
        return jsonify({'error': 'not_found', 'jan': jan}), 404

    # 値入率計算
    if product.get('sell_price') and product['sell_price'] > 0 and product.get('cost') and product['cost'] > 0:
        product['margin_rate'] = round((1 - product['cost'] / product['sell_price']) * 100, 1)
    else:
        product['margin_rate'] = 0

    # スキャンされたJANとDB上のJANが異なる場合（インストア商品正規化時）
    product['scanned_jan'] = jan
    product['matched_jan'] = product['jan']

    return jsonify(product)


@app.route('/api/scan/search', methods=['GET'])
def scan_search():
    """商品名・メーカー名で商品を検索（部分一致）"""
    q = request.args.get('q', '').strip()
    if not q or len(q) < 2:
        return jsonify({'error': '2文字以上で検索してください'}), 400

    # スペースで分割して AND 検索
    terms = q.split()
    conditions = []
    params = []
    for term in terms[:5]:  # 最大5語
        conditions.append(
            "(product_name ILIKE %s OR product_name_kana ILIKE %s OR supplier_code ILIKE %s OR edp ILIKE %s OR jan ILIKE %s)"
        )
        like = f"%{term}%"
        params.extend([like, like, like, like, like])

    where = " AND ".join(conditions)
    sql = f"""SELECT jan, edp, product_name, product_name_kana, spec,
              dept_code, supplier_code, cost, sell_price, tax_price
              FROM t_scan_products
              WHERE {where}
              ORDER BY product_name
              LIMIT 50"""

    rows = query_rows(sql, params)

    # 値入率を計算
    for r in rows:
        if r.get('sell_price') and r['sell_price'] > 0 and r.get('cost') and r['cost'] > 0:
            r['margin_rate'] = round((1 - r['cost'] / r['sell_price']) * 100, 1)
        else:
            r['margin_rate'] = 0

    return jsonify({'results': rows, 'count': len(rows), 'query': q})


@app.route('/api/scan/history', methods=['GET'])
def scan_history():
    """JANコードの販売履歴（日別・店舗別）"""
    jan = request.args.get('jan', '').strip()
    store = request.args.get('store', '')

    if not jan:
        return jsonify({'error': 'jan parameter required'}), 400

    query = """SELECT date, store_code, sales_qty, sales_amount,
               discount_count, discount_amount, cost_amount
               FROM t_scan_pos_daily WHERE jan = %s"""
    params = [jan]

    if store:
        query += " AND store_code = %s"
        params.append(store)

    query += " ORDER BY date DESC, store_code"
    rows = query_rows(query, params)

    for r in rows:
        r['store_name'] = STORE_NAMES.get(r['store_code'], '店舗' + str(r['store_code']))

    # 日別合計
    daily = {}
    for r in rows:
        d = r['date']
        if d not in daily:
            daily[d] = {'date': d, 'total_qty': 0, 'total_amount': 0}
        daily[d]['total_qty'] += r.get('sales_qty', 0) or 0
        daily[d]['total_amount'] += r.get('sales_amount', 0) or 0

    # 週別集計
    dates_sorted = sorted(daily.keys(), reverse=True)
    weekly = []
    for i in range(0, min(len(dates_sorted), 28), 7):
        week_dates = dates_sorted[i:i+7]
        wk = {'week': len(weekly) + 1, 'qty': 0, 'amount': 0, 'days': len(week_dates)}
        for d in week_dates:
            wk['qty'] += daily[d]['total_qty']
            wk['amount'] += daily[d]['total_amount']
        weekly.append(wk)

    return jsonify({
        'jan': jan,
        'detail': rows,
        'daily': sorted(daily.values(), key=lambda x: x['date'], reverse=True),
        'weekly': weekly,
        'store_names': STORE_NAMES,
    })


# ========== 受払いAPI ==========

@app.route('/api/scan/ukebarai', methods=['GET'])
def scan_ukebarai():
    """JANコードの受払いデータ"""
    jan = request.args.get('jan', '').strip()
    store = request.args.get('store', '')

    if not jan:
        return jsonify({'error': 'jan parameter required'}), 400

    query = """SELECT date, store_code, product_name, sell_price, cost_price,
               pos_qty, order_qty, purchase_qty, transfer_in_qty,
               transfer_out_qty, return_qty, disposal_qty
               FROM t_scan_ukebarai WHERE jan = %s"""
    params = [jan]

    if store:
        query += " AND store_code = %s"
        params.append(store)

    query += " ORDER BY date DESC, store_code LIMIT 200"
    rows = query_rows(query, params)

    for r in rows:
        r['store_name'] = STORE_NAMES.get(r['store_code'], '店舗' + str(r['store_code']))

    return jsonify({'jan': jan, 'data': rows, 'store_names': STORE_NAMES})


# ========== 在庫API ==========

@app.route('/api/scan/zaiko', methods=['GET'])
def scan_zaiko():
    """JANコードの在庫データ（全店舗）"""
    jan = request.args.get('jan', '').strip()

    if not jan:
        return jsonify({'error': 'jan parameter required'}), 400

    rows = query_rows("""SELECT store_code, store_name, product_name,
               prev_stock, purchase_qty, pos_qty, transfer_in_qty,
               transfer_out_qty, theory_stock, actual_stock, current_stock,
               last_purchase_date, stock_sell_amount, stock_cost_amount
               FROM t_scan_zaiko WHERE jan = %s
               ORDER BY store_code""", [jan])

    # 全店合計
    total = {
        'prev_stock': 0, 'purchase_qty': 0, 'pos_qty': 0,
        'transfer_in_qty': 0, 'transfer_out_qty': 0,
        'theory_stock': 0, 'current_stock': 0,
        'stock_sell_amount': 0, 'stock_cost_amount': 0
    }
    for r in rows:
        for k in total:
            total[k] += (r.get(k) or 0)

    return jsonify({'jan': jan, 'stores': rows, 'total': total})


# ========== デバッグ: 受払い生データ確認 ==========

@app.route('/api/scan/debug_ukebarai', methods=['GET'])
def debug_ukebarai():
    """受払いDBの生データ確認（デバッグ用）"""
    # 仕入数量 > 0 のデータを探す
    rows_with_purchase = query_rows("""
        SELECT date, store_code, jan, product_name,
               sell_price, cost_price, pos_qty, order_qty,
               purchase_qty, transfer_in_qty, transfer_out_qty,
               return_qty, disposal_qty, col14, col15,
               ext_purchase_cost, ext_purchase_sell, ext_purchase_qty,
               ext_return_cost, ext_return_sell, ext_return_qty
        FROM t_scan_ukebarai
        WHERE purchase_qty > 0
        ORDER BY date DESC
        LIMIT 10
    """)
    # 全データサンプル（先頭10件）
    sample_rows = query_rows("""
        SELECT date, store_code, jan, product_name,
               sell_price, cost_price, pos_qty, order_qty,
               purchase_qty, transfer_in_qty, transfer_out_qty,
               return_qty, disposal_qty, col14, col15
        FROM t_scan_ukebarai
        ORDER BY date DESC
        LIMIT 10
    """)
    # 各カラムの非0件数
    col_stats = query_one("""
        SELECT
            COUNT(*) as total_rows,
            SUM(CASE WHEN pos_qty != 0 THEN 1 ELSE 0 END) as has_pos_qty,
            SUM(CASE WHEN order_qty != 0 THEN 1 ELSE 0 END) as has_order_qty,
            SUM(CASE WHEN purchase_qty != 0 THEN 1 ELSE 0 END) as has_purchase_qty,
            SUM(CASE WHEN transfer_in_qty != 0 THEN 1 ELSE 0 END) as has_transfer_in,
            SUM(CASE WHEN transfer_out_qty != 0 THEN 1 ELSE 0 END) as has_transfer_out,
            SUM(CASE WHEN return_qty != 0 THEN 1 ELSE 0 END) as has_return_qty,
            SUM(CASE WHEN disposal_qty != 0 THEN 1 ELSE 0 END) as has_disposal_qty,
            SUM(CASE WHEN col14 != 0 THEN 1 ELSE 0 END) as has_col14,
            SUM(CASE WHEN col15 != 0 THEN 1 ELSE 0 END) as has_col15,
            SUM(CASE WHEN ext_purchase_qty != 0 THEN 1 ELSE 0 END) as has_ext_purchase
        FROM t_scan_ukebarai
    """)
    return jsonify({
        'column_stats': col_stats,
        'rows_with_purchase': rows_with_purchase,
        'sample_rows': sample_rows,
    })


# ========== 棚割りAPI ==========

@app.route('/api/scan/tanawari', methods=['GET'])
def scan_tanawari():
    """JANコードの棚割りデータ（全店舗）"""
    jan = request.args.get('jan', '').strip()

    if not jan:
        return jsonify({'error': 'jan parameter required'}), 400

    rows = query_rows("""SELECT store_code, gondola_no, shelf_no, position,
               product_name, face_count, edp, dept_name, sub_dept_name,
               supplier_code, supplier_name, cost, sell_price, margin_rate
               FROM t_scan_tanawari WHERE jan = %s
               ORDER BY store_code, gondola_no, shelf_no, position""", [jan])

    for r in rows:
        r['store_name'] = STORE_NAMES.get(r['store_code'], '店舗' + str(r['store_code']))

    return jsonify({'jan': jan, 'data': rows, 'store_names': STORE_NAMES})


# ========== 統計API ==========

@app.route('/api/scan/stores', methods=['GET'])
def scan_stores():
    return jsonify(STORE_NAMES)


@app.route('/api/scan/stats', methods=['GET'])
def scan_stats():
    try:
        prod = query_one("SELECT COUNT(*) as cnt FROM t_scan_products")
        prod_count = prod['cnt'] if prod else 0
    except:
        prod_count = 0
    try:
        pos = query_one("SELECT COUNT(*) as cnt, MIN(date) as d_min, MAX(date) as d_max FROM t_scan_pos_daily")
        pos_count = pos['cnt'] if pos else 0
        date_from = pos['d_min'] if pos else None
        date_to = pos['d_max'] if pos else None
    except:
        pos_count = 0
        date_from = date_to = None
    try:
        uke = query_one("SELECT COUNT(*) as cnt FROM t_scan_ukebarai")
        uke_count = uke['cnt'] if uke else 0
    except:
        uke_count = 0
    try:
        zaiko = query_one("SELECT COUNT(*) as cnt FROM t_scan_zaiko")
        zaiko_count = zaiko['cnt'] if zaiko else 0
    except:
        zaiko_count = 0

    return jsonify({
        'product_count': prod_count,
        'pos_record_count': pos_count,
        'ukebarai_count': uke_count,
        'zaiko_count': zaiko_count,
        'date_from': date_from,
        'date_to': date_to,
    })


# ========== FTP同期 ==========

FTP_HOST = os.environ.get('FTP_HOST', '')
FTP_PORT = int(os.environ.get('FTP_PORT', '21'))
FTP_USER = os.environ.get('FTP_USER', '')
FTP_PASS = os.environ.get('FTP_PASS', '')
FTP_REMOTE_DIR = os.environ.get('FTP_REMOTE_DIR', '/old')

_ftp_sync_status = {
    'running': False,
    'last_run': None,
    'last_result': None,
    'log': [],
}
_ftp_sync_lock = threading.Lock()

# 取込対象ファイル
TARGET_PREFIXES = ['posd', 'syohin', 'baika', 'ukebarai', 'zaiko', 'tanawari']


def _ftp_log(msg):
    ts = datetime.now().strftime('%H:%M:%S')
    entry = f"[{ts}] {msg}"
    _ftp_sync_status['log'].append(entry)
    print(f"[ftp-sync] {entry}", flush=True)


def _to_num(s):
    s = (s or "").strip()
    if not s:
        return 0
    try:
        return int(s)
    except ValueError:
        try:
            return float(s)
        except ValueError:
            return 0


def _jp_strip(s):
    return (s or "").strip().strip("　").strip()


def _connect_ftp():
    _ftp_log(f"FTPサーバに接続中: {FTP_HOST}:{FTP_PORT}")
    try:
        ftp = ftplib.FTP_TLS()
        ftp.connect(FTP_HOST, FTP_PORT, timeout=30)
        ftp.login(FTP_USER, FTP_PASS)
        ftp.prot_p()
        _ftp_log("FTPS (TLS) 接続成功")
        return ftp
    except Exception as e:
        _ftp_log(f"FTPS失敗 ({e})、通常FTPで再試行")
    try:
        ftp = ftplib.FTP()
        ftp.connect(FTP_HOST, FTP_PORT, timeout=30)
        ftp.login(FTP_USER, FTP_PASS)
        _ftp_log("通常FTP接続成功")
        return ftp
    except Exception as e:
        _ftp_log(f"FTP接続失敗: {e}")
        raise


def _download_csvs(ftp, tmpdir):
    """最新の各CSVファイルをダウンロード"""
    try:
        ftp.cwd(FTP_REMOTE_DIR)
    except ftplib.error_perm as e:
        _ftp_log(f"ディレクトリ移動失敗 {FTP_REMOTE_DIR}: {e}")
        return {}

    files = []
    try:
        for name, facts in ftp.mlsd():
            if facts.get("type", "") == "file":
                files.append(name)
    except (ftplib.error_perm, AttributeError):
        try:
            files = [n for n in ftp.nlst() if n not in (".", "..")]
        except ftplib.error_perm:
            _ftp_log("ファイル一覧取得失敗")
            return {}

    _ftp_log(f"リモートファイル数: {len(files)}")

    # 各プレフィックスの最新ファイルを特定
    latest = {}
    for name in files:
        for prefix in TARGET_PREFIXES:
            if name.startswith(prefix + '_') and name.endswith('.csv'):
                if prefix not in latest or name > latest[prefix]:
                    latest[prefix] = name

    downloaded = {}
    for prefix, fname in latest.items():
        local_path = os.path.join(tmpdir, prefix + '.csv')
        try:
            with open(local_path, "wb") as f:
                ftp.retrbinary(f"RETR {fname}", f.write)
            size = os.path.getsize(local_path)
            _ftp_log(f"  ダウンロード: {fname} ({size/1024:.0f} KB)")
            downloaded[prefix] = local_path
        except Exception as e:
            _ftp_log(f"  ダウンロード失敗 {fname}: {e}")

    return downloaded


def _read_sjis_csv(path, min_cols):
    try:
        with open(path, "r", encoding="cp932", errors="replace", newline="") as f:
            for row in csv.reader(f):
                if len(row) >= min_cols:
                    yield row
    except Exception as e:
        _ftp_log(f"CSV読込失敗 {os.path.basename(path)}: {e}")


def _sync_products(tmpdir, downloaded):
    """syohin.csv + baika.csv → t_scan_products"""
    if 'syohin' not in downloaded:
        _ftp_log("syohin.csv なし: 商品マスタ更新スキップ")
        return 0

    _ftp_log("商品マスタ(syohin.csv) 読込中...")
    products = {}
    for r in _read_sjis_csv(downloaded['syohin'], 30):
        jan = _jp_strip(r[3])
        if len(jan) < 8:
            continue
        products[jan] = {
            'jan': jan, 'edp': _jp_strip(r[0]),
            'product_name': _jp_strip(r[12]),
            'product_name_kana': _jp_strip(r[10]),
            'spec': _jp_strip(r[13]),
            'dept_code': f"{_jp_strip(r[14])}-{_jp_strip(r[15])}-{_jp_strip(r[16])}-{_jp_strip(r[17])}",
            'dept_l': _jp_strip(r[14]), 'dept_m': _jp_strip(r[15]),
            'dept_s': _jp_strip(r[16]),
            'supplier_code': _jp_strip(r[9]),
            'sell_price': _to_num(r[28]), 'cost': 0, 'tax_price': 0,
        }
    _ftp_log(f"  商品マスタ: {len(products)} 件読込")

    if 'baika' in downloaded:
        _ftp_log("売価マスタ(baika.csv) 読込中...")
        baika_count = 0
        for r in _read_sjis_csv(downloaded['baika'], 7):
            jan = _jp_strip(r[0])
            if jan in products:
                cost_val = _to_num(r[3])
                sell = _to_num(r[4])
                tax_price = _to_num(r[6])
                if cost_val > 0:
                    products[jan]['cost'] = cost_val
                if tax_price > 0:
                    products[jan]['tax_price'] = tax_price
                if sell > 0 and products[jan]['sell_price'] == 0:
                    products[jan]['sell_price'] = sell
                baika_count += 1
        _ftp_log(f"  売価データ: {baika_count} 件適用")

    conn = get_db()
    cur = conn.cursor()
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    cur.execute("DELETE FROM t_scan_products")
    for p in products.values():
        cur.execute("""INSERT INTO t_scan_products
            (jan, edp, product_name, product_name_kana, spec,
             dept_code, dept_l, dept_m, dept_s, supplier_code,
             cost, sell_price, tax_price, updated_at)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (jan) DO UPDATE SET
            edp=EXCLUDED.edp, product_name=EXCLUDED.product_name,
            product_name_kana=EXCLUDED.product_name_kana, spec=EXCLUDED.spec,
            dept_code=EXCLUDED.dept_code, dept_l=EXCLUDED.dept_l,
            dept_m=EXCLUDED.dept_m, dept_s=EXCLUDED.dept_s,
            supplier_code=EXCLUDED.supplier_code, cost=EXCLUDED.cost,
            sell_price=EXCLUDED.sell_price, tax_price=EXCLUDED.tax_price,
            updated_at=EXCLUDED.updated_at""",
            (p['jan'], p['edp'], p['product_name'], p['product_name_kana'],
             p['spec'], p['dept_code'], p['dept_l'], p['dept_m'], p['dept_s'],
             p['supplier_code'], p['cost'], p['sell_price'], p['tax_price'], now))
    conn.commit()
    cur.close()
    conn.close()
    _ftp_log(f"商品マスタ更新完了: {len(products)} 件")
    return len(products)


def _sync_pos(tmpdir, downloaded):
    """posd.csv → t_scan_pos_daily"""
    if 'posd' not in downloaded:
        _ftp_log("posd.csv なし: POS日別更新スキップ")
        return 0

    _ftp_log("全店POS日別(posd.csv) 読込中...")
    agg = {}
    for r in _read_sjis_csv(downloaded['posd'], 10):
        key = (_jp_strip(r[0]), _jp_strip(r[1]), _jp_strip(r[2]))
        if key not in agg:
            agg[key] = {'edp': _jp_strip(r[3]), 'qty': 0, 'amt': 0, 'dc': 0, 'da': 0, 'cost': 0}
        a = agg[key]
        a['qty'] += _to_num(r[5])
        a['amt'] += _to_num(r[6])
        a['dc'] += _to_num(r[7])
        a['da'] += _to_num(r[8])
        a['cost'] += _to_num(r[9])

    dates = set(k[0] for k in agg)
    _ftp_log(f"  POS日別: {len(agg)} 件 (日付: {len(dates)}日分)")

    conn = get_db()
    cur = conn.cursor()
    for dt in dates:
        cur.execute("DELETE FROM t_scan_pos_daily WHERE date = %s", (dt,))

    for k, v in agg.items():
        cur.execute("""INSERT INTO t_scan_pos_daily
            (date, store_code, jan, edp, sales_qty, sales_amount,
             discount_count, discount_amount, cost_amount)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
            (k[0], k[1], k[2], v['edp'], v['qty'], v['amt'], v['dc'], v['da'], v['cost']))
    conn.commit()
    total = query_one("SELECT COUNT(*) as cnt FROM t_scan_pos_daily")
    cur.close()
    conn.close()
    _ftp_log(f"POS日別更新完了: {len(agg)} 件 (総行数: {total['cnt']})")
    return len(agg)


def _sync_ukebarai(tmpdir, downloaded):
    """ukebarai.csv → t_scan_ukebarai"""
    if 'ukebarai' not in downloaded:
        _ftp_log("ukebarai.csv なし: 受払い更新スキップ")
        return 0

    _ftp_log("受払い(ukebarai.csv) 読込中...")

    # デバッグ: CSVの先頭3行をログに出力してカラム構造を確認
    debug_count = 0
    for r in _read_sjis_csv(downloaded['ukebarai'], 4):
        _ftp_log(f"[CSV構造確認] 行{debug_count}: 列数={len(r)}, データ={r[:30]}")
        debug_count += 1
        if debug_count >= 3:
            break

    count = 0
    conn = get_db()
    cur = conn.cursor()

    dates_seen = set()
    rows_batch = []
    for r in _read_sjis_csv(downloaded['ukebarai'], 7):
        date = _jp_strip(r[0])
        store = _jp_strip(r[1])
        jan = _jp_strip(r[2])
        if len(jan) < 3:
            continue
        dates_seen.add(date)

        # === CHAINS受払いCSV 30列 正確なカラムマッピング ===
        # （Excelヘッダー「日別受払いデータ」より確定）
        # r[0]=日付  r[1]=店コード  r[2]=代表スキャニングコード  r[3]=商品名
        # r[4]=売上金額  r[5]=売上原価金額  r[6]=売上数量
        # r[7]=買上客数
        # r[8]=廃棄金額  r[9]=廃棄原価金額  r[10]=廃棄数量
        # r[11]=値引金額  r[12]=値引数量
        # r[13]=値上金額  r[14]=値上数量
        # r[15]=仕入原価金額  r[16]=仕入売価金額  r[17]=仕入数量
        # r[18]=仕入返品原価金額  r[19]=仕入返品売価金額  r[20]=仕入返品数量
        # r[21]=店間移動原価金額  r[22]=店間移動売価金額  r[23]=店間移動数量
        # r[24]=部門移動原価金額  r[25]=部門移動売価金額  r[26]=部門移動数量
        # r[27]=総仕入原価金額  r[28]=総仕入売価金額  r[29]=総仕入数量

        pos_qty = _to_num(r[6]) if len(r) > 6 else 0       # 売上数量
        order_qty = _to_num(r[7]) if len(r) > 7 else 0     # 買上客数(order_qtyカラムに格納)
        purchase_qty = int(_to_num(r[17])) if len(r) > 17 else 0    # 仕入数量
        transfer_in_qty = int(_to_num(r[23])) if len(r) > 23 else 0 # 店間移動数量
        transfer_out_qty = int(_to_num(r[26])) if len(r) > 26 else 0 # 部門移動数量
        return_qty = int(_to_num(r[20])) if len(r) > 20 else 0      # 仕入返品数量
        disposal_qty = _to_num(r[10]) if len(r) > 10 else 0  # 廃棄数量 ※r[10]が正しい
        markdown_amount = _to_num(r[11]) if len(r) > 11 else 0  # 値引金額
        markdown_qty = _to_num(r[12]) if len(r) > 12 else 0     # 値引数量

        rows_batch.append((
            date, store, jan, _jp_strip(r[3]),
            _to_num(r[4]), _to_num(r[5]),           # sell_price, cost_price
            pos_qty, order_qty, purchase_qty,
            transfer_in_qty, transfer_out_qty, return_qty,
            disposal_qty, markdown_amount, markdown_qty,
            # ext_purchase_cost/sell/qty → 0 (purchase_qtyに直接格納済み)
            0, 0, 0,
            # ext_return_cost/sell/qty → 0 (return_qtyに直接格納済み)
            0, 0, 0,
        ))
        count += 1

    # 日付単位で既存データ削除してINSERT
    for dt in dates_seen:
        cur.execute("DELETE FROM t_scan_ukebarai WHERE date = %s", (dt,))

    for row in rows_batch:
        cur.execute("""INSERT INTO t_scan_ukebarai
            (date, store_code, jan, product_name, sell_price, cost_price,
             pos_qty, order_qty, purchase_qty, transfer_in_qty,
             transfer_out_qty, return_qty, disposal_qty, col14, col15,
             ext_purchase_cost, ext_purchase_sell, ext_purchase_qty,
             ext_return_cost, ext_return_sell, ext_return_qty)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""", row)

    conn.commit()
    total = query_one("SELECT COUNT(*) as cnt FROM t_scan_ukebarai")
    cur.close()
    conn.close()
    _ftp_log(f"受払い更新完了: {count} 件 (総行数: {total['cnt']})")
    return count


def _sync_zaiko(tmpdir, downloaded):
    """zaiko.csv → t_scan_zaiko"""
    if 'zaiko' not in downloaded:
        _ftp_log("zaiko.csv なし: 在庫更新スキップ")
        return 0

    _ftp_log("在庫(zaiko.csv) 読込中...")
    count = 0
    conn = get_db()
    cur = conn.cursor()
    cur.execute("DELETE FROM t_scan_zaiko")
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    for r in _read_sjis_csv(downloaded['zaiko'], 18):
        store = _jp_strip(r[0])
        jan = _jp_strip(r[4])
        if len(jan) < 8:
            continue
        cur.execute("""INSERT INTO t_scan_zaiko
            (store_code, store_name, dept_code, edp, jan, product_name,
             prev_stock, purchase_qty, pos_qty, transfer_in_qty,
             transfer_out_qty, theory_stock, actual_stock, current_stock,
             col15, last_purchase_date, stock_sell_amount, stock_cost_amount, updated_at)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (store_code, jan) DO UPDATE SET
            prev_stock=EXCLUDED.prev_stock, purchase_qty=EXCLUDED.purchase_qty,
            pos_qty=EXCLUDED.pos_qty, theory_stock=EXCLUDED.theory_stock,
            current_stock=EXCLUDED.current_stock, stock_sell_amount=EXCLUDED.stock_sell_amount,
            stock_cost_amount=EXCLUDED.stock_cost_amount, updated_at=EXCLUDED.updated_at""",
            (store, _jp_strip(r[1]), _jp_strip(r[2]), _jp_strip(r[3]),
             jan, _jp_strip(r[5]),
             _to_num(r[6]), _to_num(r[7]), _to_num(r[8]),
             _to_num(r[9]), _to_num(r[10]), _to_num(r[11]),
             _to_num(r[12]), _to_num(r[13]), _to_num(r[14]),
             _jp_strip(r[15]), _to_num(r[16]), _to_num(r[17]), now))
        count += 1

    conn.commit()
    cur.close()
    conn.close()
    _ftp_log(f"在庫更新完了: {count} 件")
    return count


def _sync_tanawari(tmpdir, downloaded):
    """tanawari.csv → t_scan_tanawari"""
    if 'tanawari' not in downloaded:
        _ftp_log("tanawari.csv なし: 棚割更新スキップ")
        return 0

    _ftp_log("棚割(tanawari.csv) 読込中...")
    count = 0
    conn = get_db()
    cur = conn.cursor()
    cur.execute("DELETE FROM t_scan_tanawari")

    for r in _read_sjis_csv(downloaded['tanawari'], 16):
        jan = _jp_strip(r[4])
        if len(jan) < 8:
            continue
        cur.execute("""INSERT INTO t_scan_tanawari
            (store_code, gondola_no, shelf_no, position, jan, product_name,
             face_count, edp, dept_code, dept_name, sub_dept_code,
             sub_dept_name, supplier_code, supplier_name, cost, sell_price, margin_rate)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (store_code, gondola_no, shelf_no, position) DO UPDATE SET
            jan=EXCLUDED.jan, product_name=EXCLUDED.product_name,
            face_count=EXCLUDED.face_count, sell_price=EXCLUDED.sell_price""",
            (_jp_strip(r[0]), _jp_strip(r[1]), _jp_strip(r[2]), _to_num(r[3]),
             jan, _jp_strip(r[5]), _to_num(r[6]), _jp_strip(r[7]),
             _jp_strip(r[8]), _jp_strip(r[9]), _jp_strip(r[10]),
             _jp_strip(r[11]), _jp_strip(r[12]), _jp_strip(r[13]),
             _to_num(r[14]), _to_num(r[15]), _to_num(r[16])))
        count += 1

    conn.commit()
    cur.close()
    conn.close()
    _ftp_log(f"棚割更新完了: {count} 件")
    return count


def _run_ftp_sync():
    """FTP同期の本体"""
    _ftp_sync_status['log'] = []
    _ftp_sync_status['running'] = True
    _ftp_sync_status['last_run'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    tmpdir = tempfile.mkdtemp(prefix="scan_sync_")
    try:
        _ftp_log("=== FTP同期 開始 ===")
        ftp = _connect_ftp()
        try:
            downloaded = _download_csvs(ftp, tmpdir)
        finally:
            try:
                ftp.quit()
            except Exception:
                try:
                    ftp.close()
                except:
                    pass

        _ftp_log(f"ダウンロード完了: {len(downloaded)} ファイル ({', '.join(downloaded.keys())})")

        scan_products = _sync_products(tmpdir, downloaded)
        scan_pos = _sync_pos(tmpdir, downloaded)
        scan_uke = _sync_ukebarai(tmpdir, downloaded)
        scan_zaiko = _sync_zaiko(tmpdir, downloaded)
        scan_tana = _sync_tanawari(tmpdir, downloaded)

        _ftp_log("=== FTP同期 完了 ===")
        _ftp_sync_status['last_result'] = {
            'success': True,
            'downloaded': len(downloaded),
            'files': list(downloaded.keys()),
            'scan_products': scan_products,
            'scan_pos': scan_pos,
            'scan_ukebarai': scan_uke,
            'scan_zaiko': scan_zaiko,
            'scan_tanawari': scan_tana,
            'finished_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        }
    except Exception as e:
        import traceback
        _ftp_log(f"エラー: {e}")
        _ftp_log(traceback.format_exc())
        _ftp_sync_status['last_result'] = {
            'success': False,
            'error': str(e),
            'finished_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        }
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)
        _ftp_sync_status['running'] = False


@app.route('/api/ftp-sync', methods=['POST'])
def start_ftp_sync():
    if _ftp_sync_status['running']:
        return jsonify({'error': '同期処理が実行中です。完了までお待ちください。'}), 409

    with _ftp_sync_lock:
        if _ftp_sync_status['running']:
            return jsonify({'error': '同期処理が実行中です'}), 409
        t = threading.Thread(target=_run_ftp_sync, daemon=True)
        t.start()

    return jsonify({'message': 'FTP同期を開始しました', 'status': 'started'})


@app.route('/api/ftp-sync/status', methods=['GET'])
def get_ftp_sync_status():
    return jsonify({
        'running': _ftp_sync_status['running'],
        'last_run': _ftp_sync_status['last_run'],
        'last_result': _ftp_sync_status['last_result'],
        'log': _ftp_sync_status['log'][-50:],
    })


# ========== 日次自動同期スケジューラー ==========

DAILY_SYNC_HOUR_JST = 8  # 日本時間 午前8時に自動同期（FTPデータ更新6-7時の後）

def _daily_sync_scheduler():
    """毎日指定時刻(JST)にFTP同期を自動実行するバックグラウンドスレッド"""
    import time
    from datetime import timezone, timedelta
    JST = timezone(timedelta(hours=9))

    # 起動直後は少し待ってから開始
    time.sleep(10)
    print(f"[日次同期] スケジューラー起動 (毎日 JST {DAILY_SYNC_HOUR_JST}:00 に実行)", flush=True)

    last_sync_date = None

    while True:
        try:
            now_jst = datetime.now(JST)
            today_str = now_jst.strftime('%Y-%m-%d')

            # 今日の同期がまだで、指定時刻を過ぎていれば実行
            if now_jst.hour >= DAILY_SYNC_HOUR_JST and last_sync_date != today_str:
                # 既に手動同期が実行中なら待つ
                if _ftp_sync_status['running']:
                    print(f"[日次同期] 同期実行中のためスキップ、60秒後に再確認", flush=True)
                    time.sleep(60)
                    continue

                print(f"[日次同期] {today_str} の自動同期を開始します", flush=True)
                _ftp_sync_status['log'] = []  # ログクリア
                try:
                    _run_ftp_sync()
                    last_sync_date = today_str
                    result = _ftp_sync_status.get('last_result', {})
                    if result.get('success'):
                        print(f"[日次同期] 完了: 商品{result.get('scan_products',0)} POS{result.get('scan_pos',0)} 受払{result.get('scan_ukebarai',0)} 在庫{result.get('scan_zaiko',0)}", flush=True)
                    else:
                        print(f"[日次同期] エラー: {result.get('error','不明')}", flush=True)
                except Exception as e:
                    print(f"[日次同期] 実行エラー: {e}", flush=True)
                    last_sync_date = today_str  # エラーでも今日は再試行しない

            # 次のチェックまで10分待機
            time.sleep(600)

        except Exception as e:
            print(f"[日次同期] スケジューラーエラー: {e}", flush=True)
            time.sleep(600)


@app.route('/api/sync-schedule', methods=['GET'])
def get_sync_schedule():
    """自動同期スケジュール情報を返す"""
    from datetime import timezone, timedelta
    JST = timezone(timedelta(hours=9))
    now_jst = datetime.now(JST)
    return jsonify({
        'enabled': True,
        'schedule': f'毎日 JST {DAILY_SYNC_HOUR_JST}:00',
        'current_time_jst': now_jst.strftime('%Y-%m-%d %H:%M:%S'),
        'last_sync': _ftp_sync_status.get('last_run'),
        'last_result': _ftp_sync_status.get('last_result'),
    })


# ========== 起動 ==========

def _safe_init():
    """DB初期化・自動同期をバックグラウンドで安全に実行。
    gunicornのポートバインドをブロックしない。"""
    import time
    time.sleep(2)  # gunicornがポートバインドする時間を確保

    try:
        ensure_tables()
        print(f"[初期化完了] PostgreSQL接続済み", flush=True)
    except Exception as e:
        print(f"[初期化エラー] テーブル作成失敗: {e}", flush=True)
        print("[初期化エラー] 60秒後に再試行します", flush=True)
        time.sleep(60)
        try:
            ensure_tables()
            print(f"[初期化完了] PostgreSQL接続済み (再試行成功)", flush=True)
        except Exception as e2:
            print(f"[初期化エラー] 再試行も失敗: {e2}", flush=True)
            return

    # 起動時に商品マスタが空なら自動FTP同期
    try:
        result = query_one("SELECT COUNT(*) as cnt FROM t_scan_products")
        if result and result['cnt'] == 0:
            print("[自動同期] DBが空のためFTP同期を自動実行します", flush=True)
            _run_ftp_sync()
        else:
            print(f"[自動同期] 商品 {result['cnt']} 件存在 → スキップ", flush=True)
    except Exception as e:
        print(f"[自動同期エラー] {e}", flush=True)

if DATABASE_URL:
    # 初期化をバックグラウンドスレッドで実行（ポートバインドをブロックしない）
    _init_thread = threading.Thread(target=_safe_init, daemon=True)
    _init_thread.start()

    # 日次自動同期スケジューラーを起動
    _daily_sync_thread = threading.Thread(target=_daily_sync_scheduler, daemon=True)
    _daily_sync_thread.start()
    print(f"[日次同期] スケジューラー登録済み (毎日 JST {DAILY_SYNC_HOUR_JST}:00)", flush=True)
else:
    print("[警告] DATABASE_URL未設定。PostgreSQLに接続できません。", flush=True)

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    print(f"サーバー起動: http://localhost:{port}")
    app.run(host='0.0.0.0', port=port, debug=True)
