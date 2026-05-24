"""
マルエーうちや 商品スキャンアプリ (PWA)
JANコードで商品情報・販売実績を照会
CHAINS基幹FTPからデータ取込
"""
import os
import sys
import csv
import sqlite3
import ftplib
import hashlib
import tempfile
import shutil
import threading
from datetime import datetime
from flask import Flask, request, jsonify, Response

# ========== アプリ設定 ==========

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.environ.get('DATA_DIR', BASE_DIR)
DB_PATH = os.path.join(DATA_DIR, 'scan.db')

app = Flask(__name__)

# ========== DB ユーティリティ ==========

def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def rows_to_list(rows):
    return [dict(r) for r in rows]


def ensure_tables():
    """商品マスタ・POS日別テーブルを作成"""
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("""CREATE TABLE IF NOT EXISTS t_scan_products (
        jan TEXT PRIMARY KEY,
        edp TEXT,
        product_name TEXT,
        product_name_kana TEXT,
        spec TEXT,
        dept_code TEXT,
        dept_l TEXT,
        dept_m TEXT,
        dept_s TEXT,
        supplier_code TEXT,
        cost REAL DEFAULT 0,
        sell_price INTEGER DEFAULT 0,
        tax_price INTEGER DEFAULT 0,
        updated_at TEXT
    )""")
    conn.execute("""CREATE TABLE IF NOT EXISTS t_scan_pos_daily (
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
    conn.execute("CREATE INDEX IF NOT EXISTS idx_scan_pos_jan ON t_scan_pos_daily(jan)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_scan_pos_date ON t_scan_pos_daily(date)")
    conn.commit()
    conn.close()


# ========== 店舗マスタ ==========

STORE_NAMES = {
    '5': '泉店', '6': '平面店', '7': '旭南店', '8': '御所野店',
    '9': '酒田北店', '10': '鶴岡店', '11': '本荘石脇店', '12': '茨島店', '13': '鶴岡南店'
}


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

@app.route('/api/scan/product', methods=['GET'])
def scan_product():
    """JANコードで商品情報を検索"""
    jan = request.args.get('jan', '').strip()
    if not jan:
        return jsonify({'error': 'jan parameter required'}), 400

    conn = get_db()
    product = conn.execute("SELECT * FROM t_scan_products WHERE jan = ?", (jan,)).fetchone()
    if not product:
        conn.close()
        return jsonify({'error': 'not_found', 'jan': jan}), 404

    result = dict(product)

    # 売価と原価から値入率を計算
    if result.get('sell_price') and result['sell_price'] > 0 and result.get('cost') and result['cost'] > 0:
        result['margin_rate'] = round((1 - result['cost'] / result['sell_price']) * 100, 1)
    else:
        result['margin_rate'] = 0

    conn.close()
    return jsonify(result)


@app.route('/api/scan/history', methods=['GET'])
def scan_history():
    """JANコードの販売履歴（日別・店舗別）を取得"""
    jan = request.args.get('jan', '').strip()
    days = int(request.args.get('days', '28'))
    store = request.args.get('store', '')

    if not jan:
        return jsonify({'error': 'jan parameter required'}), 400

    conn = get_db()
    query = """SELECT date, store_code, sales_qty, sales_amount,
               discount_count, discount_amount, cost_amount
               FROM t_scan_pos_daily WHERE jan = ?"""
    params = [jan]

    if store:
        query += " AND store_code = ?"
        params.append(store)

    query += " ORDER BY date DESC, store_code"
    rows = rows_to_list(conn.execute(query, params).fetchall())

    # 店舗名を付与
    for r in rows:
        r['store_name'] = STORE_NAMES.get(r['store_code'], '店舗' + r['store_code'])

    # 日別合計（全店）
    daily = {}
    for r in rows:
        d = r['date']
        if d not in daily:
            daily[d] = {'date': d, 'total_qty': 0, 'total_amount': 0}
        daily[d]['total_qty'] += r.get('sales_qty', 0)
        daily[d]['total_amount'] += r.get('sales_amount', 0)

    # 週別集計（4週分）
    dates_sorted = sorted(daily.keys(), reverse=True)
    weekly = []
    for i in range(0, min(len(dates_sorted), 28), 7):
        week_dates = dates_sorted[i:i+7]
        wk = {'week': len(weekly) + 1, 'qty': 0, 'amount': 0, 'days': len(week_dates)}
        for d in week_dates:
            wk['qty'] += daily[d]['total_qty']
            wk['amount'] += daily[d]['total_amount']
        weekly.append(wk)

    conn.close()
    return jsonify({
        'jan': jan,
        'detail': rows,
        'daily': sorted(daily.values(), key=lambda x: x['date'], reverse=True),
        'weekly': weekly,
        'store_names': STORE_NAMES,
    })


@app.route('/api/scan/stores', methods=['GET'])
def scan_stores():
    """店舗一覧"""
    return jsonify(STORE_NAMES)


@app.route('/api/scan/stats', methods=['GET'])
def scan_stats():
    """統計情報"""
    conn = get_db()
    try:
        prod_count = conn.execute("SELECT COUNT(*) FROM t_scan_products").fetchone()[0]
    except:
        prod_count = 0
    try:
        pos_count = conn.execute("SELECT COUNT(*) FROM t_scan_pos_daily").fetchone()[0]
        date_range = conn.execute("SELECT MIN(date), MAX(date) FROM t_scan_pos_daily").fetchone()
    except:
        pos_count = 0
        date_range = (None, None)
    conn.close()
    return jsonify({
        'product_count': prod_count,
        'pos_record_count': pos_count,
        'date_from': date_range[0],
        'date_to': date_range[1],
    })


# ========== FTP同期 ==========

FTP_HOST = os.environ.get('FTP_HOST', 'o6021v-1001.kagoya.net')
FTP_PORT = int(os.environ.get('FTP_PORT', '21'))
FTP_USER = os.environ.get('FTP_USER', 'kir700301..tisc')
FTP_PASS = os.environ.get('FTP_PASS', 'tiscmaruei2026')
FTP_REMOTE_DIR = os.environ.get('FTP_REMOTE_DIR', '/old')

_ftp_sync_status = {
    'running': False,
    'last_run': None,
    'last_result': None,
    'log': [],
}
_ftp_sync_lock = threading.Lock()

TARGET_CSV_FILES = ["posd.csv", "syohin.csv", "baika.csv"]


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


def _is_target_csv(filename):
    fn = filename.lower()
    for t in TARGET_CSV_FILES:
        base = t.rsplit(".", 1)[0]
        if fn == t or fn.endswith("_" + t) or (base in fn and fn.endswith(".csv")):
            return t
    return None


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
    try:
        ftp.cwd(FTP_REMOTE_DIR)
    except ftplib.error_perm as e:
        _ftp_log(f"ディレクトリ移動失敗 {FTP_REMOTE_DIR}: {e}")
        return []

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
            return []

    _ftp_log(f"リモートファイル数: {len(files)}")
    downloaded = []
    for name in files:
        target_type = _is_target_csv(name)
        if not target_type:
            continue
        local_path = os.path.join(tmpdir, target_type)
        try:
            with open(local_path, "wb") as f:
                ftp.retrbinary(f"RETR {name}", f.write)
            size = os.path.getsize(local_path)
            _ftp_log(f"  ダウンロード: {name} → {target_type} ({size:,} bytes)")
            downloaded.append(target_type)
        except Exception as e:
            _ftp_log(f"  ダウンロード失敗 {name}: {e}")

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
    syohin_path = os.path.join(tmpdir, "syohin.csv")
    if "syohin.csv" not in downloaded or not os.path.exists(syohin_path):
        _ftp_log("syohin.csv なし: 商品マスタ更新スキップ")
        return 0

    _ftp_log("商品マスタ(syohin.csv) 読込中...")
    products = {}
    for r in _read_sjis_csv(syohin_path, 30):
        jan = _jp_strip(r[3])
        if len(jan) < 8:
            continue
        products[jan] = {
            'jan': jan,
            'edp': _jp_strip(r[0]),
            'product_name': _jp_strip(r[12]),
            'product_name_kana': _jp_strip(r[10]),
            'spec': _jp_strip(r[13]),
            'dept_code': f"{_jp_strip(r[14])}-{_jp_strip(r[15])}-{_jp_strip(r[16])}-{_jp_strip(r[17])}",
            'dept_l': _jp_strip(r[14]),
            'dept_m': _jp_strip(r[15]),
            'dept_s': _jp_strip(r[16]),
            'supplier_code': _jp_strip(r[9]),
            'sell_price': _to_num(r[28]),
            'cost': 0,
            'tax_price': 0,
        }
    _ftp_log(f"  商品マスタ: {len(products)} 件読込")

    # baika.csv で原価・税込売価を補完
    baika_path = os.path.join(tmpdir, "baika.csv")
    if "baika.csv" in downloaded and os.path.exists(baika_path):
        _ftp_log("売価マスタ(baika.csv) 読込中...")
        baika_count = 0
        for r in _read_sjis_csv(baika_path, 7):
            jan = _jp_strip(r[0])
            if jan in products:
                cost_rate = float(r[3]) if r[3] else 0
                sell = _to_num(r[4])
                tax_price = _to_num(r[6])
                if sell > 0 and cost_rate > 0:
                    products[jan]['cost'] = round(sell * cost_rate / 100, 2)
                if tax_price > 0:
                    products[jan]['tax_price'] = tax_price
                if sell > 0 and products[jan]['sell_price'] == 0:
                    products[jan]['sell_price'] = sell
                baika_count += 1
        _ftp_log(f"  売価データ: {baika_count} 件適用")

    # DB書き込み
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    conn.execute("DELETE FROM t_scan_products")
    rows = [(p['jan'], p['edp'], p['product_name'], p['product_name_kana'],
             p['spec'], p['dept_code'], p['dept_l'], p['dept_m'], p['dept_s'],
             p['supplier_code'], p['cost'], p['sell_price'], p['tax_price'], now)
            for p in products.values()]
    conn.executemany("""INSERT OR REPLACE INTO t_scan_products
        (jan, edp, product_name, product_name_kana, spec,
         dept_code, dept_l, dept_m, dept_s, supplier_code,
         cost, sell_price, tax_price, updated_at)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", rows)
    conn.commit()
    conn.close()
    _ftp_log(f"商品マスタ更新完了: {len(rows)} 件")
    return len(rows)


def _sync_pos(tmpdir, downloaded):
    """posd.csv → t_scan_pos_daily"""
    posd_path = os.path.join(tmpdir, "posd.csv")
    if "posd.csv" not in downloaded or not os.path.exists(posd_path):
        _ftp_log("posd.csv なし: POS日別更新スキップ")
        return 0

    _ftp_log("全店POS日別(posd.csv) 読込中...")
    agg = {}
    for r in _read_sjis_csv(posd_path, 10):
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
    _ftp_log(f"  POS日別: {len(agg)} 件 (日付: {sorted(dates)})")

    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    for dt in dates:
        conn.execute("DELETE FROM t_scan_pos_daily WHERE date = ?", (dt,))

    rows = [(k[0], k[1], k[2], v['edp'], v['qty'], v['amt'], v['dc'], v['da'], v['cost'])
            for k, v in agg.items()]
    conn.executemany("""INSERT INTO t_scan_pos_daily
        (date, store_code, jan, edp, sales_qty, sales_amount,
         discount_count, discount_amount, cost_amount)
        VALUES (?,?,?,?,?,?,?,?,?)""", rows)
    conn.commit()
    total = conn.execute("SELECT COUNT(*) FROM t_scan_pos_daily").fetchone()[0]
    conn.close()
    _ftp_log(f"POS日別更新完了: {len(rows)} 件登録 (総行数: {total})")
    return len(rows)


def _run_ftp_sync():
    """FTP同期の本体（バックグラウンドスレッドで実行）"""
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
                ftp.close()

        _ftp_log(f"ダウンロード完了: {len(downloaded)} ファイル ({', '.join(downloaded)})")

        scan_products = _sync_products(tmpdir, downloaded)
        scan_pos = _sync_pos(tmpdir, downloaded)

        _ftp_log("=== FTP同期 完了 ===")
        _ftp_sync_status['last_result'] = {
            'success': True,
            'downloaded': len(downloaded),
            'files': downloaded,
            'scan_products': scan_products,
            'scan_pos': scan_pos,
            'finished_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        }

    except Exception as e:
        _ftp_log(f"エラー: {e}")
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


# ========== 起動 ==========

ensure_tables()
print(f"[初期化完了] DB: {DB_PATH}")

# 起動時にDBが空なら自動FTP同期
def _auto_sync_on_startup():
    """サーバー起動時にDBが空なら自動的にFTP同期を実行"""
    import time
    time.sleep(3)  # サーバー起動を少し待つ
    try:
        conn = sqlite3.connect(DB_PATH)
        count = conn.execute("SELECT COUNT(*) FROM t_scan_products").fetchone()[0]
        conn.close()
        if count == 0:
            print("[自動同期] DBが空のためFTP同期を自動実行します", flush=True)
            _run_ftp_sync()
        else:
            print(f"[自動同期] 商品 {count} 件存在 → スキップ", flush=True)
    except Exception as e:
        print(f"[自動同期エラー] {e}", flush=True)

# バックグラウンドで自動同期を起動
_auto_sync_thread = threading.Thread(target=_auto_sync_on_startup, daemon=True)
_auto_sync_thread.start()

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    print(f"サーバー起動: http://localhost:{port}")
    app.run(host='0.0.0.0', port=port, debug=True)
