import os
import json
import math
from datetime import datetime
from dateutil import tz

import pandas as pd
import yfinance as yf


# -------------------------
# Helpers
# -------------------------
def fmt_volume(n: float) -> str:
    if n is None or (isinstance(n, float) and math.isnan(n)):
        return ""
    n = float(n)
    if n >= 1_000_000_000:
        return f"{n/1_000_000_000:.2f}b"
    if n >= 1_000_000:
        return f"{n/1_000_000:.2f}m"
    if n >= 1_000:
        return f"{n/1_000:.2f}k"
    return str(int(n))


def pct_change(close: float, prev_close: float) -> float:
    if prev_close in (None, 0) or pd.isna(prev_close) or pd.isna(close):
        return float("nan")
    return (close - prev_close) / prev_close * 100.0


def trend_label(pct: float) -> str:
    if pd.isna(pct):
        return ""
    return "increase" if pct > 0 else "decrease"


def build_trend_note(o, h, l, c, prev_c, vol, prev_vol=None) -> str:
    """
    Simple rule-based note (fast & consistent).
    You can later replace this with an AI call if you want.
    """
    if any(pd.isna(x) for x in [o, h, l, c, prev_c]):
        return ""

    day_range = (h - l) / prev_c * 100 if prev_c else 0
    chg = pct_change(c, prev_c)

    volatility = "high" if day_range >= 5 else "moderate" if day_range >= 2 else "low"
    direction = "rebounded" if chg > 0 else "sold off" if chg < 0 else "closed flat"

    vol_phrase = ""
    if prev_vol and prev_vol > 0 and not pd.isna(prev_vol) and not pd.isna(vol):
        if vol >= prev_vol * 1.2:
            vol_phrase = " on higher volume"
        elif vol <= prev_vol * 0.8:
            vol_phrase = " on lighter volume"
        else:
            vol_phrase = " on steady volume"

    bias = "bullish" if chg >= 2 else "bearish" if chg <= -2 else "neutral"
    return f"{direction}{vol_phrase} with {volatility} intraday volatility; short-term bias is {bias}."


def get_latest_daily_bar(ticker: str) -> pd.DataFrame:
    """
    Fetch recent daily bars and return the last 2 rows so we can compute prev close & compare volume.
    """
    df = yf.download(ticker, period="10d", interval="1d", auto_adjust=False, progress=False)
    if df is None or df.empty:
        return pd.DataFrame()

    # yfinance sometimes returns multiindex columns
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [c[0] for c in df.columns]

    df = df.dropna(subset=["Close"])
    if len(df) == 0:
        return pd.DataFrame()

    # keep last 2 trading days
    return df.tail(2).copy()


def try_get_company_name(ticker: str) -> str:
    try:
        info = yf.Ticker(ticker).info
        name = info.get("shortName") or info.get("longName") or ""
        return name.strip()
    except Exception:
        return ""


def append_unique_csv(path: str, new_rows: pd.DataFrame, key_cols=("Date", "Ticker")) -> None:
    if os.path.exists(path):
        old = pd.read_csv(path, dtype=str)
        combined = pd.concat([old, new_rows.astype(str)], ignore_index=True)
        combined = combined.drop_duplicates(subset=list(key_cols), keep="last")
    else:
        combined = new_rows.astype(str)

    # Keep in chronological order (Date is dd/mm/yyyy)
    def parse_date(d):
        try:
            return datetime.strptime(d, "%d/%m/%Y")
        except Exception:
            return datetime.min

    combined["__date"] = combined["Date"].apply(parse_date)
    combined = combined.sort_values(["__date", "Ticker"]).drop(columns=["__date"])
    combined.to_csv(path, index=False)


# -------------------------
# Google Sheets (optional)
# -------------------------
def append_to_google_sheet(rows: pd.DataFrame) -> None:
    """
    Requires env:
      GOOGLE_SERVICE_ACCOUNT_JSON  (the service account json string)
      SHEET_ID                     (Google Sheet file ID)
      SHEET_TAB                    (optional tab name, default 'Sheet1')
    """
    sa_json = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "").strip()
    sheet_id = os.getenv("SHEET_ID", "").strip()
    tab_name = os.getenv("SHEET_TAB", "Sheet1").strip()

    if not sa_json or not sheet_id:
        print("Google Sheets not configured (missing GOOGLE_SERVICE_ACCOUNT_JSON or SHEET_ID). Skipping.")
        return

    import gspread
    from google.oauth2.service_account import Credentials

    creds_dict = json.loads(sa_json)
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)

    gc = gspread.authorize(creds)
    sh = gc.open_by_key(sheet_id)

    try:
        ws = sh.worksheet(tab_name)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=tab_name, rows=2000, cols=30)

    existing = ws.get_all_values()
    if not existing:
        ws.append_row(list(rows.columns), value_input_option="RAW")

    # Append row by row (safe and simple)
    for _, r in rows.iterrows():
        ws.append_row([str(r[c]) for c in rows.columns], value_input_option="RAW")


# -------------------------
# Main
# -------------------------
def main():
    watchlist_path = os.getenv("WATCHLIST", "watchlist.csv")
    out_csv = os.getenv("OUT_CSV", "stock_log.csv")
    tz_sg = tz.gettz("Asia/Singapore")

    wl = pd.read_csv(watchlist_path).fillna("")
    if "Ticker" not in wl.columns:
        raise ValueError("watchlist.csv must have a 'Ticker' column")

    rows = []
    for _, item in wl.iterrows():
        ticker = str(item["Ticker"]).strip()
        if not ticker:
            continue

        name = str(item.get("CompanyName", "")).strip()

        bars = get_latest_daily_bar(ticker)
        if bars.empty or len(bars) < 1:
            print(f"[WARN] No data for {ticker}")
            continue

        latest = bars.iloc[-1]
        prev = bars.iloc[-2] if len(bars) >= 2 else None

        # yfinance index is timezone-naive; treat it as exchange day and format dd/mm/yyyy
        trade_date = bars.index[-1].to_pydatetime().astimezone(tz_sg) if bars.index.tzinfo else bars.index[-1].to_pydatetime()
        date_str = trade_date.strftime("%d/%m/%Y")

        o = float(latest.get("Open", float("nan")))
        h = float(latest.get("High", float("nan")))
        l = float(latest.get("Low", float("nan")))
        c = float(latest.get("Close", float("nan")))
        v = float(latest.get("Volume", float("nan")))

        prev_close = float(prev.get("Close")) if prev is not None else float("nan")
        prev_vol = float(prev.get("Volume")) if prev is not None else None

        pchg = pct_change(c, prev_close)
        trend = trend_label(pchg)

        if not name:
            name = try_get_company_name(ticker) or ticker

        note = build_trend_note(o, h, l, c, prev_close, v, prev_vol)

        row = {
            "Date": date_str,
            "Ticker": ticker,
            "Company Name": name,
            "Open": round(o, 2) if not pd.isna(o) else "",
            "High": round(h, 2) if not pd.isna(h) else "",
            "Low": round(l, 2) if not pd.isna(l) else "",
            "Close": round(c, 2) if not pd.isna(c) else "",
            "Previous Close": round(prev_close, 2) if not pd.isna(prev_close) else "",
            "Volume": fmt_volume(v),
            "% Change (Day)": f"{pchg:.2f}%" if not pd.isna(pchg) else "",
            "Trend Note": note,
            "trend": trend,
        }
        rows.append(row)

    if not rows:
        print("No rows generated. Exiting.")
        return

    df_new = pd.DataFrame(rows)

    # Append to CSV
    append_unique_csv(out_csv, df_new, key_cols=("Date", "Ticker"))
    print(f"Updated CSV: {out_csv} (+{len(df_new)} rows, de-duped by Date+Ticker)")

    # Optional: append to Google Sheet
    append_to_google_sheet(df_new)


if __name__ == "__main__":
    main()
