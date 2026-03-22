import streamlit as st
import os
import json
import base64
import re
from datetime import datetime, timedelta, timezone
from collections import defaultdict
from email.utils import parsedate_to_datetime

from google.oauth2 import service_account
from googleapiclient.discovery import build

# ── Konfiguráció ──────────────────────────────────────────────────────────────
SERVICE_ACCOUNT_FILE = os.environ.get("SERVICE_ACCOUNT_FILE", "service_account.json")
DELEGATED_EMAIL      = os.environ.get("DELEGATED_EMAIL", "sales@deak.hu")
SCOPES               = ["https://www.googleapis.com/auth/gmail.readonly"]

st.set_page_config(
    page_title="Email Dashboard – sales@deak.hu",
    page_icon="📧",
    layout="wide",
)

st.markdown("""
<style>
.metric-card {
    background: #f8f9fa;
    border-radius: 12px;
    padding: 20px 24px;
    text-align: center;
    border: 1px solid #e9ecef;
}
.metric-label { font-size: 13px; color: #6c757d; margin-bottom: 4px; }
.metric-value { font-size: 32px; font-weight: 600; color: #212529; }
.metric-sub   { font-size: 12px; color: #adb5bd; margin-top: 2px; }
.section-title {
    font-size: 15px; font-weight: 600; color: #495057;
    margin: 24px 0 12px; padding-bottom: 6px;
    border-bottom: 1px solid #e9ecef;
}
</style>
""", unsafe_allow_html=True)


# ── Auth ──────────────────────────────────────────────────────────────────────
@st.cache_resource
def get_gmail_service():
    if not os.path.exists(SERVICE_ACCOUNT_FILE):
        return None, "service_account_missing"
    try:
        creds = service_account.Credentials.from_service_account_file(
            SERVICE_ACCOUNT_FILE, scopes=SCOPES
        )
        delegated = creds.with_subject(DELEGATED_EMAIL)
        service = build("gmail", "v1", credentials=delegated)
        service.users().getProfile(userId="me").execute()
        return service, "ok"
    except Exception as e:
        return None, str(e)


# ── Gmail helpers ─────────────────────────────────────────────────────────────
def get_header(headers, name):
    for h in headers:
        if h["name"].lower() == name.lower():
            return h["value"]
    return ""

def parse_date(date_str):
    if not date_str:
        return None
    try:
        return parsedate_to_datetime(date_str)
    except Exception:
        return None

def fetch_messages(service, query, max_results=500):
    messages, page_token = [], None
    while True:
        params = {
            "userId": "me",
            "q": query,
            "maxResults": min(max_results - len(messages), 100),
        }
        if page_token:
            params["pageToken"] = page_token
        resp = service.users().messages().list(**params).execute()
        messages.extend(resp.get("messages", []))
        page_token = resp.get("nextPageToken")
        if not page_token or len(messages) >= max_results:
            break
    return messages

def get_message_meta(service, msg_id):
    msg = service.users().messages().get(
        userId="me", id=msg_id, format="metadata",
        metadataHeaders=["From", "To", "Subject", "Date"],
    ).execute()
    headers = msg.get("payload", {}).get("headers", [])
    return {
        "id":       msg_id,
        "from":     get_header(headers, "From"),
        "to":       get_header(headers, "To"),
        "subject":  get_header(headers, "Subject"),
        "date":     parse_date(get_header(headers, "Date")),
        "threadId": msg.get("threadId", ""),
        "labelIds": msg.get("labelIds", []),
    }

def fetch_period_data(service, start_dt, end_dt, max_results):
    after  = int(start_dt.timestamp())
    before = int(end_dt.timestamp())

    with st.spinner("Bejövő levelek lekérése…"):
        in_ids   = fetch_messages(service, f"after:{after} before:{before} in:inbox", max_results)
    with st.spinner("Küldött levelek lekérése…"):
        sent_ids = fetch_messages(service, f"after:{after} before:{before} in:sent", max_results)

    inbox, sent = [], []
    total = len(in_ids) + len(sent_ids)
    prog  = st.progress(0, text=f"Metaadatok betöltése… 0/{total}")
    done  = 0

    for m in in_ids:
        inbox.append(get_message_meta(service, m["id"]))
        done += 1
        prog.progress(done / max(total, 1), text=f"Feldolgozva: {done}/{total}")

    for m in sent_ids:
        sent.append(get_message_meta(service, m["id"]))
        done += 1
        prog.progress(done / max(total, 1), text=f"Feldolgozva: {done}/{total}")

    prog.empty()
    return inbox, sent


# ── Analitika ─────────────────────────────────────────────────────────────────
def extract_email_addr(raw):
    m = re.search(r"<([^>]+)>", raw)
    return m.group(1).lower() if m else raw.strip().lower()

def analyze(inbox, sent):
    sent_thread_ids = {m["threadId"] for m in sent}
    unanswered = [m for m in inbox if m["threadId"] not in sent_thread_ids]

    sender_count = defaultdict(int)
    for m in inbox:
        sender_count[extract_email_addr(m["from"])] += 1
    top_senders = sorted(sender_count.items(), key=lambda x: -x[1])[:15]

    subject_count = defaultdict(int)
    for m in inbox:
        subj = re.sub(r"^(re:|fwd?:|aw:)\s*", "", m["subject"], flags=re.IGNORECASE).strip()
        key  = " ".join(subj.split()[:5]) or "(nincs tárgy)"
        subject_count[key] += 1
    top_subjects = sorted(subject_count.items(), key=lambda x: -x[1])[:15]

    inbox_by_thread = defaultdict(list)
    for m in inbox:
        if m["date"]:
            inbox_by_thread[m["threadId"]].append(m["date"])

    sent_by_thread = defaultdict(list)
    for m in sent:
        if m["date"]:
            sent_by_thread[m["threadId"]].append(m["date"])

    response_times = []
    for tid, in_dates in inbox_by_thread.items():
        if tid in sent_by_thread:
            first_in   = min(in_dates)
            first_sent = min(sent_by_thread[tid])
            if first_sent > first_in:
                diff_h = (first_sent - first_in).total_seconds() / 3600
                if diff_h < 168:
                    response_times.append(diff_h)

    avg_resp = sum(response_times) / len(response_times) if response_times else None
    med_resp = sorted(response_times)[len(response_times) // 2] if response_times else None

    return {
        "unanswered":   unanswered,
        "top_senders":  top_senders,
        "top_subjects": top_subjects,
        "avg_resp_h":   avg_resp,
        "med_resp_h":   med_resp,
        "resp_count":   len(response_times),
    }

def fmt_hours(h):
    if h is None:
        return "—"
    if h < 1:
        return f"{int(h * 60)} perc"
    if h < 24:
        return f"{h:.1f} óra"
    return f"{h / 24:.1f} nap"


# ── Auth ellenőrzés ───────────────────────────────────────────────────────────
service, auth_status = get_gmail_service()

if auth_status == "service_account_missing":
    st.error("Nem található `service_account.json`. Lásd a README telepítési útmutatót.")
    with st.expander("Telepítési útmutató"):
        st.markdown("""
1. **Google Cloud Console** → új projekt (pl. `deak-email-dashboard`)
2. **APIs & Services → Library** → Gmail API engedélyezése
3. **APIs & Services → Credentials → Create Credentials → Service Account**
   - Neve: pl. `email-dashboard-sa`
   - Szerepkör: nincs szükség rá → kész
4. A létrehozott service account sorában: **Keys → Add Key → JSON** → letöltés → mentsd `service_account.json` névvel az app mellé
5. **Google Workspace Admin Console** → Security → API controls → Domain-wide Delegation → **Add new**
   - Client ID: a service account numerikus Client ID-ja (a JSON-ban: `client_id`)
   - OAuth scopes: `https://www.googleapis.com/auth/gmail.readonly`
6. Indítsd újra az appot
""")
    st.stop()
elif auth_status != "ok":
    st.error(f"Hitelesítési hiba: {auth_status}")
    with st.expander("Hibaelhárítás"):
        st.markdown("""
- Ellenőrizd, hogy a Domain-wide Delegation be van-e állítva az Admin Console-ban
- A service account JSON fájlban szereplő `client_email` mezőt másold ki és ellenőrizd
- A `DELEGATED_EMAIL` környezeti változó értéke egyezzen a figyelni kívánt postaládával
- A Gmail API engedélyezve van-e az adott Google Cloud projektben?
""")
    st.stop()


# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.header("⚙️ Beállítások")
    st.success(f"✓ Kapcsolódva: **{DELEGATED_EMAIL}**")

    mode = st.radio("Időszak", ["Aktuális hét", "Előző hét", "Aktuális hónap", "Egyéni"])

    now = datetime.now(timezone.utc)

    if mode == "Aktuális hét":
        start = (now - timedelta(days=now.weekday())).replace(hour=0, minute=0, second=0, microsecond=0)
        end   = now
    elif mode == "Előző hét":
        start = (now - timedelta(days=now.weekday() + 7)).replace(hour=0, minute=0, second=0, microsecond=0)
        end   = start + timedelta(days=7)
    elif mode == "Aktuális hónap":
        start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        end   = now
    else:
        c1, c2 = st.columns(2)
        with c1:
            d1 = st.date_input("Kezdet", value=(now - timedelta(days=7)).date())
        with c2:
            d2 = st.date_input("Vég", value=now.date())
        start = datetime.combine(d1, datetime.min.time(), tzinfo=timezone.utc)
        end   = datetime.combine(d2, datetime.max.time(), tzinfo=timezone.utc)

    max_msgs = st.slider("Max. levelek száma", 100, 2000, 500, 100,
                         help="Nagyobb szám = pontosabb, de lassabb futás")
    run_btn  = st.button("🔄 Összesítés futtatása", use_container_width=True)


# ── Fejléc ────────────────────────────────────────────────────────────────────
st.title("📧 Email forgalom összesítő")
st.caption(f"**{DELEGATED_EMAIL}** · {start.strftime('%Y. %m. %d.')} – {end.strftime('%Y. %m. %d.')}")


# ── Futtatás ──────────────────────────────────────────────────────────────────
if run_btn:
    inbox, sent = fetch_period_data(service, start, end, max_msgs)
    stats = analyze(inbox, sent)
    st.session_state["result"] = {
        "inbox": inbox, "sent": sent, "stats": stats,
        "start": start, "end": end,
    }

if "result" not in st.session_state:
    st.info("Válaszd ki az időszakot, majd kattints az **Összesítés futtatása** gombra.")
    st.stop()

res   = st.session_state["result"]
inbox = res["inbox"]
sent  = res["sent"]
stats = res["stats"]


# ── Metrika kártyák ───────────────────────────────────────────────────────────
c1, c2, c3, c4 = st.columns(4)
for col, label, val, unit in [
    (c1, "📥 Beérkezett",     len(inbox),               "levél"),
    (c2, "📤 Küldött",        len(sent),                "levél"),
    (c3, "⏳ Válaszolatlan",  len(stats["unanswered"]), "levél"),
    (c4, "⚡ Átl. válaszidő", fmt_hours(stats["avg_resp_h"]), ""),
]:
    with col:
        st.markdown(f"""
        <div class="metric-card">
          <div class="metric-label">{label}</div>
          <div class="metric-value">{val}</div>
          <div class="metric-sub">{unit}</div>
        </div>""", unsafe_allow_html=True)

st.markdown("---")

left, right = st.columns(2)

with left:
    st.markdown('<div class="section-title">👤 Top feladók</div>', unsafe_allow_html=True)
    if stats["top_senders"]:
        max_c = stats["top_senders"][0][1]
        for addr, cnt in stats["top_senders"]:
            st.markdown(f"""
            <div style="margin-bottom:8px;">
              <div style="display:flex;justify-content:space-between;font-size:13px;margin-bottom:3px;">
                <span style="color:#495057;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;max-width:80%">{addr}</span>
                <span style="color:#6c757d;font-weight:600">{cnt}</span>
              </div>
              <div style="background:#e9ecef;border-radius:4px;height:5px;">
                <div style="background:#4361ee;width:{cnt/max_c*100:.0f}%;height:5px;border-radius:4px;"></div>
              </div>
            </div>""", unsafe_allow_html=True)
    else:
        st.caption("Nincs adat")

    st.markdown('<div class="section-title">⏱ Válaszidő részletek</div>', unsafe_allow_html=True)
    r1, r2, r3 = st.columns(3)
    r1.metric("Átlagos",     fmt_hours(stats["avg_resp_h"]))
    r2.metric("Medián",      fmt_hours(stats["med_resp_h"]))
    r3.metric("Mért szálak", stats["resp_count"])

with right:
    st.markdown('<div class="section-title">📋 Leggyakoribb témák</div>', unsafe_allow_html=True)
    if stats["top_subjects"]:
        max_c = stats["top_subjects"][0][1]
        for subj, cnt in stats["top_subjects"]:
            st.markdown(f"""
            <div style="margin-bottom:8px;">
              <div style="display:flex;justify-content:space-between;font-size:13px;margin-bottom:3px;">
                <span style="color:#495057;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;max-width:80%">{subj}</span>
                <span style="color:#6c757d;font-weight:600">{cnt}</span>
              </div>
              <div style="background:#e9ecef;border-radius:4px;height:5px;">
                <div style="background:#7209b7;width:{cnt/max_c*100:.0f}%;height:5px;border-radius:4px;"></div>
              </div>
            </div>""", unsafe_allow_html=True)
    else:
        st.caption("Nincs adat")


# ── Válaszolatlan levelek ─────────────────────────────────────────────────────
st.markdown('<div class="section-title">⏳ Válaszolatlan levelek</div>', unsafe_allow_html=True)
if stats["unanswered"]:
    import pandas as pd
    rows = []
    for m in sorted(stats["unanswered"],
                    key=lambda x: x["date"] or datetime.min.replace(tzinfo=timezone.utc),
                    reverse=True)[:100]:
        rows.append({
            "Dátum":  m["date"].strftime("%Y-%m-%d %H:%M") if m["date"] else "—",
            "Feladó": extract_email_addr(m["from"]),
            "Tárgy":  m["subject"][:90],
        })
    st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)
else:
    st.success("Nincs válaszolatlan levél ebben az időszakban.")


# ── Export ────────────────────────────────────────────────────────────────────
st.markdown("---")
if st.button("📥 Összesítés letöltése (JSON)"):
    export = {
        "account":               DELEGATED_EMAIL,
        "period":                {"start": res["start"].isoformat(), "end": res["end"].isoformat()},
        "inbox_count":           len(inbox),
        "sent_count":            len(sent),
        "unanswered_count":      len(stats["unanswered"]),
        "avg_response_hours":    stats["avg_resp_h"],
        "median_response_hours": stats["med_resp_h"],
        "top_senders":           stats["top_senders"],
        "top_subjects":          stats["top_subjects"],
    }
    b64 = base64.b64encode(json.dumps(export, ensure_ascii=False, indent=2).encode()).decode()
    fn  = f"email_summary_{res['start'].strftime('%Y%m%d')}_{res['end'].strftime('%Y%m%d')}.json"
    st.markdown(
        f'<a href="data:application/json;base64,{b64}" download="{fn}">⬇️ Kattints ide a letöltéshez</a>',
        unsafe_allow_html=True,
    )
