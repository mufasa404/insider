import requests
import pandas as pd
import yfinance as yf
import time
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from datetime import datetime

# --- KONFIGURATION ---
URL = "https://www.dataroma.com/m/ins/ins.php"

# Filter-Einstellungen
MIN_INVEST_ABSOLUT = 2_000_000  # Ab 2 Mio $
MIN_RELATIV_IMPACT = 0.1  # Ab 0.1% der Firma
CHECK_INTERVAL = 100  # Alle 15 Minuten prüfen (900 Sekunden)

# --- EMAIL KONFIGURATION ---
SMTP_SERVER = "smtp.gmail.com"
PORT = 587
ABSENDER_EMAIL = "profit.pilot.404@gmail.com"
EMPFAENGER_EMAIL = "d.j.wiesmann@gmail.com"
# WICHTIG: Hier dein App-Passwort eintragen (das aus deinem Snippet)
PASSWORT = "lgrx vdnb eqjf dndi"

# --- GEDÄCHTNIS ---
# Verhindert doppelte Emails für den gleichen Trade
SEEN_TRADES = set()

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
}


def send_email(trade_data):
    """Deine SMTP-Logik angepasst für Insider-Daten"""

    # E-Mail Inhalt schön formatieren
    betreff = f"🚨 INSIDER ALARM: {trade_data['Ticker']} ({trade_data['Signale']})"

    nachricht_text = f"""
    NEUER HIGH-IMPACT INSIDER TRADE GEFUNDEN!
    
    ------------------------------------------
    Firma:       {trade_data['Firma']} ({trade_data['Ticker']})
    Datum:       {trade_data['Datum']}
    Käufer:      {trade_data['Käufer']}
    
    Investiert:  ${trade_data['Invest ($)']:,.2f}
    Market Cap:  ~${trade_data['MarketCap']:,.2f}
    IMPACT:      {trade_data['Impact (%)']:.3f}% der Firma gekauft
    
    Signale:     {trade_data['Signale']}
    ------------------------------------------
    
    Link zu Yahoo: https://finance.yahoo.com/quote/{trade_data['Ticker']}
    """

    # Erstellen der E-Mail-Nachricht (MIMEMultipart wie in deinem Code)
    msg = MIMEMultipart()
    msg["From"] = ABSENDER_EMAIL
    msg["To"] = EMPFAENGER_EMAIL
    msg["Subject"] = betreff
    msg.attach(MIMEText(nachricht_text, "plain"))

    try:
        # Verbindung zum SMTP-Server herstellen und E-Mail senden
        server = smtplib.SMTP(SMTP_SERVER, PORT)
        server.starttls()  # Startet die verschlüsselte Verbindung
        server.login(ABSENDER_EMAIL, PASSWORT)
        server.sendmail(ABSENDER_EMAIL, EMPFAENGER_EMAIL, msg.as_string())
        print(f"✅ E-Mail erfolgreich gesendet für {trade_data['Ticker']}!")
    except Exception as e:
        print(f"❌ Fehler beim Senden der E-Mail: {e}")
    finally:
        try:
            server.quit()
        except:
            pass  # Falls Verbindung schon weg ist


def get_market_cap(ticker):
    try:
        stock = yf.Ticker(ticker)
        return stock.fast_info.market_cap
    except:
        return None


def get_dataroma_data():
    try:
        # Timeout hinzugefügt, damit der Server nicht hängt
        response = requests.get(URL, headers=headers, timeout=20)
        tables = pd.read_html(response.text, match="Filing")
        if not tables:
            return None
        df = tables[0]

        df.columns = (
            df.columns.astype(str).str.replace(r"[▲▼]", "", regex=True).str.strip()
        )

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
        print(f"⚠️ Netzwerk/Parsing Fehler: {e}")
        return None


def process_market_cycle():
    """Ein Prüf-Durchlauf"""
    print(f"\n--- Prüfe Markt um {datetime.now().strftime('%H:%M:%S')} ---")

    df = get_dataroma_data()
    if df is None or df.empty:
        return

    # Grobfilter
    candidates = df[df["Invested_Amount"] > MIN_INVEST_ABSOLUT].copy()

    if candidates.empty:
        print("Keine Trades über der absoluten Hürde gefunden.")
        return

    for index, row in candidates.iterrows():
        ticker = row["Symbol"]
        amount = row["Invested_Amount"]
        buyer = row["Reporting Name"]
        date = row["Filing"]

        # ID erstellen (Doppler-Schutz)
        trade_id = f"{ticker}_{date}_{buyer}_{amount}"

        if trade_id in SEEN_TRADES:
            continue

        print(f"Analysiere neuen Kandidaten: {ticker}...", end="\r")

        mcap = get_market_cap(ticker)
        impact_percent = 0
        if mcap and mcap > 0:
            impact_percent = (amount / mcap) * 100

        # --- FILTER LOGIK ---
        is_relevant = False
        reason = []

        if amount > 20_000_000:
            is_relevant = True
            reason.append("WHALE")

        if impact_percent > MIN_RELATIV_IMPACT:
            is_relevant = True
            reason.append(f"IMPACT (>{MIN_RELATIV_IMPACT}%)")

        if "CFO" in str(buyer) or "Chief Financial" in str(buyer):
            if impact_percent > 0.05:
                is_relevant = True
                reason.append("CFO INSIDER")

        if is_relevant:
            print(
                f"\n🔥 TREFFER: {ticker} | Impact: {impact_percent:.2f}% | Sende Mail..."
            )

            # Daten für die Email vorbereiten
            trade_data = {
                "Datum": date,
                "Ticker": ticker,
                "Firma": row["Security"],
                "Käufer": buyer,
                "Invest ($)": amount,
                "MarketCap": mcap if mcap else 0,
                "Impact (%)": impact_percent,
                "Signale": ", ".join(reason),
            }

            # Deine Email-Funktion aufrufen
            send_email(trade_data)

            SEEN_TRADES.add(trade_id)


if __name__ == "__main__":
    print("🚀 Insider-Bot gestartet. Drücke Ctrl+C zum Beenden.")
    print(
        f"Einstellungen: Min ${MIN_INVEST_ABSOLUT:,.0f} | Min Impact {MIN_RELATIV_IMPACT}%"
    )

    while True:
        try:
            process_market_cycle()
        except Exception as e:
            print(f"\n💥 Unerwarteter Fehler im Hauptloop: {e}")

        # Warten
        time.sleep(CHECK_INTERVAL)
