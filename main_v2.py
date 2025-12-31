import requests
import pandas as pd
import yfinance as yf
import time
import smtplib
import logging
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from datetime import datetime, timedelta
from io import StringIO

# --- KONFIGURATION ---
URL = "https://www.dataroma.com/m/ins/ins.php"

# Filter-Einstellungen
MIN_INVEST_ABSOLUT = 200_000  # Ab 200.000 $
CHECK_INTERVAL = 900  # Alle 15 Minuten prüfen

# --- EMAIL KONFIGURATION ---
SMTP_SERVER = "smtp.gmail.com"
PORT = 587
ABSENDER_EMAIL = "profit.pilot.404@gmail.com"
EMPFAENGER_EMAIL = "d.j.wiesmann@gmail.com"
PASSWORT = "lgrx vdnb eqjf dndi"

# --- LOGGING KONFIGURATION (Nur Konsole) ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

# --- GEDÄCHTNIS ---
SEEN_TRADES = set()

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
}


def send_email(trade_data):
    """Sendet die Email bei einem Treffer"""
    betreff = (
        f"🚨 CFO DIRECT BUY: {trade_data['Ticker']} (${trade_data['Invest ($)']:,.0f})"
    )

    nachricht_text = f"""
    NEUER CFO DIRECT TRADE GEFUNDEN!
    
    ------------------------------------------
    Firma:       {trade_data['Firma']} ({trade_data['Ticker']})
    Position:    {trade_data['Position']}
    Typ:         {trade_data['Typ']} (Direct)
    
    Transaktion: {trade_data['TransDatum']}
    Gemeldet am: {trade_data['MeldeDatum']} (Differenz: {trade_data['Zeitversatz']} Tage)
    
    Investiert:  ${trade_data['Invest ($)']:,.2f}
    Market Cap:  ~${trade_data['MarketCap']:,.2f}
    ------------------------------------------
    
    Link zu Yahoo: https://finance.yahoo.com/quote/{trade_data['Ticker']}
    """

    msg = MIMEMultipart()
    msg["From"] = ABSENDER_EMAIL
    msg["To"] = EMPFAENGER_EMAIL
    msg["Subject"] = betreff
    msg.attach(MIMEText(nachricht_text, "plain"))

    try:
        server = smtplib.SMTP(SMTP_SERVER, PORT)
        server.starttls()
        server.login(ABSENDER_EMAIL, PASSWORT)
        server.sendmail(ABSENDER_EMAIL, EMPFAENGER_EMAIL, msg.as_string())
        logging.info(f"✅ E-Mail erfolgreich gesendet für {trade_data['Ticker']}!")
        server.quit()
    except Exception as e:
        logging.error(f"❌ Fehler beim Senden der E-Mail: {e}")


def get_market_cap(ticker):
    try:
        stock = yf.Ticker(ticker)
        return stock.fast_info.market_cap
    except:
        return None


def get_dataroma_data():
    try:
        # Timeout ist wichtig, falls Dataroma langsam ist
        response = requests.get(URL, headers=headers, timeout=20)

        if response.status_code != 200:
            logging.error(f"Status Code Fehler: {response.status_code}")
            return None

        html_data = StringIO(response.text)

        # --- WICHTIGE ÄNDERUNG: match="Filing" wie in deinem Snippet ---
        # Das stellt sicher, dass wir die richtige Tabelle erwischen
        tables = pd.read_html(html_data, match="Filing")

        if not tables:
            logging.warning("Keine Tabelle mit 'Filing' gefunden.")
            return None

        df = tables[0]

        # Spalten bereinigen (Pfeile wegmachen etc.)
        df.columns = (
            df.columns.astype(str).str.replace(r"[▲▼]", "", regex=True).str.strip()
        )

        # Geld-Spalte parsen (Value oder Amount)
        money_col = next((c for c in df.columns if "Value" in c or "Amount" in c), None)

        if money_col:
            df["Invested_Amount"] = (
                df[money_col].astype(str).str.replace(r"[$,]", "", regex=True)
            )
            df["Invested_Amount"] = pd.to_numeric(
                df["Invested_Amount"], errors="coerce"
            ).fillna(0)
        else:
            df["Invested_Amount"] = 0

        return df
    except Exception as e:
        logging.error(f"⚠️ Netzwerk/Parsing Fehler: {e}")
        return None


def process_market_cycle():
    logging.info("--- Starte neuen Prüf-Zyklus ---")

    df = get_dataroma_data()
    if df is None or df.empty:
        logging.info("Keine Daten erhalten. Warte auf nächsten Zyklus.")
        return

    # Grobfilter
    candidates = df[df["Invested_Amount"] > MIN_INVEST_ABSOLUT].copy()
    count_candidates = len(candidates)

    if candidates.empty:
        logging.info("Daten geladen, aber keine Trades > 200k gefunden.")
        return
    else:
        logging.info(f"Prüfe {count_candidates} potenzielle Kandidaten (>200k)...")

    new_finds = 0

    for index, row in candidates.iterrows():
        # .get() verhindert Abstürze, falls Spaltennamen leicht variieren
        ticker = row.get("Symbol")
        amount = row.get("Invested_Amount")
        buyer = row.get("Reporting Name")

        # Datums-Felder
        filing_str = row.get("Filing Date") or row.get("Date")
        trans_str = row.get("Trans Date")

        title = str(row.get("Title", "")).upper()

        # Direct / Indirect
        ownership_type = str(row.get("D/I", "N/A")).strip().upper()

        trade_id = f"{ticker}_{filing_str}_{buyer}_{amount}"

        if trade_id in SEEN_TRADES:
            continue

        print(f"   >>> Analysiere: {ticker} ({title}) ...", end="\r")

        # --- FILTER LOGIK ---

        # 1. CFO Check
        is_cfo = "CHIEF FINANCIAL OFFICER" in title or "CFO" in title

        # 2. Direct Trade Check
        is_direct = ownership_type == "D"

        # 3. Zeit-Check (Max 24h Differenz)
        is_fast_reporting = False
        days_diff = 999

        if trans_str and filing_str:
            try:
                # Format anpassen falls nötig, aber Pandas kann das meistens automatisch
                d_filing = pd.to_datetime(filing_str)
                d_trans = pd.to_datetime(trans_str)
                days_diff = (d_filing - d_trans).days

                if days_diff <= 1:
                    is_fast_reporting = True
            except:
                is_fast_reporting = False

        # ZUSAMMENFASSUNG
        if is_cfo and is_direct and is_fast_reporting:
            print(" " * 60, end="\r")  # Zeile sauber machen
            logging.info(
                f"🔥 TREFFER: {ticker} | CFO | Direct | Delay: {days_diff}d | ${amount:,.0f}"
            )
            new_finds += 1

            mcap = get_market_cap(ticker)

            trade_data = {
                "Ticker": ticker,
                "Firma": row.get("Security", "N/A"),
                "Position": row.get("Title", "N/A"),
                "Typ": ownership_type,
                "MeldeDatum": filing_str,
                "TransDatum": trans_str,
                "Zeitversatz": days_diff,
                "Invest ($)": amount,
                "MarketCap": mcap if mcap else 0,
            }

            send_email(trade_data)
            SEEN_TRADES.add(trade_id)
        else:
            SEEN_TRADES.add(trade_id)

    print(" " * 60, end="\r")

    if new_finds == 0:
        logging.info("Zyklus beendet. Keine neuen Treffer.")
    else:
        logging.info(f"Zyklus beendet. {new_finds} Alarme versendet.")


if __name__ == "__main__":
    print("\n")
    logging.info("🚀 SYSTEM START - CFO DIRECT BOT")
    logging.info(
        f"Filter: >${MIN_INVEST_ABSOLUT:,.0f} | Nur CFO | Nur Direct | Max 24h Delay"
    )

    while True:
        try:
            process_market_cycle()
        except Exception as e:
            logging.critical(f"💥 Kritischer Fehler im Hauptloop: {e}")

        print(f"   ... Warte {CHECK_INTERVAL} Sekunden ...", end="\r")
        time.sleep(CHECK_INTERVAL)
