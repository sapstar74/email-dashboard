import streamlit as st
import json
import base64
import re
from datetime import datetime, timedelta, timezone
from collections import defaultdict
from email.utils import parsedate_to_datetime

from google.oauth2 import service_account
from googleapiclient.discovery import build

DELEGATED_EMAIL = st.secrets.get("DELEGATED_EMAIL", "sales@deak.hu")
SCOPES          = ["https://www.googleapis.com/auth/gmail.readonly"]

st.set_page_config(page_title="Email Dashboard – sales@deak.hu", page_icon="📧", layout="wide")

st.markdown("""
<style>
.metric-card { background:#f8f9fa; border-radius:12px; padding:20px 24px;
               text-align:center; border:1px solid #e9ecef; }
.metric-label { font-size:13px; color:#6c757d; margin-bottom:4px; }
.metric-value { font-size:32px; font-weight:600; color:#212529; }
.metric-sub   { font-size:12px; color:#adb5bd; margin-top:2px; }
.section-title { font-size:15px; font-weight:600; color:#495057;
                 margin:24px 0 12px; padding-bottom:6px; border-bottom:1px solid #e9ecef; }
</style>
""", unsafe_allow_html=True)


@st.cache_resource
def get_gmail_service():
    try:
        sa = dict(st.secrets["service_account"])

        # Robusztus private_key kezelés – minden lehetséges sortörés-variáns
        pk = sa.get("private_key", "")
        # Ha literális \n szöveg van benne (nem valódi sortörés), alakítsd át
        if "\\n" in pk and "\n" not in pk:
            pk = pk.replace("\\n", "\n")
        # Ha valódi sortörések vannak de hiányzik a fejléc/lábléc sortörése
        if not pk.startswith("-----BEGIN PRIVATE KEY-----\n"):
            pk = pk.replace("-----BEGIN PRIVATE KEY-----", "-----BEGIN PRIVATE KEY-----\n")
        if not pk.endswith("\n-----END PRIVATE KEY-----\n"):
            pk = pk.replace("-----END PRIVATE KEY-----", "\n-----END PRIVATE KEY-----\n")
        sa["private_key"] = pk

        creds     = service_account.Credentials.from_service_account_info(sa, scopes=SCOPES)
        delegated = creds.with_subject(DELEGATED_EMAIL)
        svc       = build("gmail", "v1", credentials=delegated)
        svc.users().getProfile(userId="me").execute()
        return svc, "ok"
    except KeyError:
        return None, "secrets_missing"
    except Exception as e:
        return None, str(e)


def get_header(headers, name):
    for h in headers:
        if h["name"].lower() == name.lower():
            return h["value"]
    return ""

def parse_date(date_str):
    try:
        return parsedate_to_datetime(date_str) if date_str else None
    except Exception:
        return None

def fetch_messages(service, query, max_results=500):
    messages, page_token = [], None
    while True:
        params = {"userId": "me", "q": query,
                  "maxResults": min(max_results - len(messages), 100)}
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
    }

def fetch_period_data(service, start_dt, end_dt, max_results):
    after, before = int(start_dt.timestamp()), int(end_dt.timestamp())
    with st.spinner("Bejövő levelek lekérése…"):
        in_ids   = fetch_messages(service, f"after:{after} before:{before} in:inbox", max_results)
    with st.spinner("Küldött levelek lekérése…"):
        sent_ids = fetch_messages(service, f"after:{after} before:{before} in:sent", max_results)

    inbox, sent, done = [], [], 0
    total = len(in_ids) + len(sent_ids)
    prog  = st.progress(0, text=f"Metaadatok: 0/{total}")
    for m in in_ids:
        inbox.append(get_message_meta(service, m["id"]))
        done += 1; prog.progress(done / max(total,1), text=f"Feldolgozva: {done}/{total}")
    for m in sent_ids:
        sent.append(get_message_meta(service, m["id"]))
        done += 1; prog.progress(done / max(total,1), text=f"Feldolgozva: {done}/{total}")
    prog.empty()
    return inbox, sent

def extract_email_addr(raw):
    m = re.search(r"<([^>]+)>", raw)
    return m.group(1).lower() if m else raw.strip().lower()

def analyze(inbox, sent):
    sent_threads = {m["threadId"] for m in sent}
    unanswered   = [m for m in inbox if m["threadId"] not in sent_threads]

    sender_count = defaultdict(int)
    for m in inbox:
        sender_count[extract_email_addr(m["from"])] += 1
    top_senders = sorted(sender_count.items(), key=lambda x: -x[1])[:15]

    subject_count = defaultdict(int)
    for m in inbox:
        subj = re.sub(r"^(re:|fwd?:|aw:)\s*", "", m["subject"], flags=re.IGNORECASE).strip()
        subject_count[" ".join(subj.split()[:5]) or "(nincs tárgy)"] += 1
    top_subjects = sorted(subject_count.items(), key=lambda x: -x[1])[:15]

    ib = defaultdict(list)
    for m in inbox:
        if m["date"]: ib[m["threadId"]].append(m["date"])
    sb = defaultdict(list)
    for m in sent:
        if m["date"]: sb[m["threadId"]].append(m["date"])

    resp_times = []
    for tid, dates in ib.items():
        if tid in sb:
            fi, fs = min(dates), min(sb[tid])
            if fs > fi:
                h = (fs - fi).total_seconds() / 3600
                if h < 168: resp_times.append(h)

    avg = sum(resp_times)/len(resp_times) if resp_times else None
    med = sorted(resp_times)[len(resp_times)//2] if resp_times else None
    return {"unanswered": unanswered, "top_senders": top_senders,
            "top_subjects": top_subjects, "avg_resp_h": avg,
            "med_resp_h": med, "resp_count": len(resp_times)}

def fmt_hours(h):
    if h is None: return "—"
    if h < 1:     return f"{int(h*60)} perc"
    if h < 24:    return f"{h:.1f} óra"
    return f"{h/24:.1f} nap"

def bar(cnt, max_c, color):
    return f"""
    <div style="margin-bottom:8px;">
      <div style="display:flex;justify-content:space-between;font-size:13px;margin-bottom:3px;">
        <span style="color:#495057;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;max-width:80%">{cnt[0]}</span>
        <span style="color:#6c757d;font-weight:600">{cnt[1]}</span>
      </div>
      <div style="background:#e9ecef;border-radius:4px;height:5px;">
        <div style="background:{color};width:{cnt[1]/max_c*100:.0f}%;height:5px;border-radius:4px;"></div>
      </div>
    </div>"""


# ── Auth ──────────────────────────────────────────────────────────────────────
service, status = get_gmail_service()

if status == "secrets_missing":
    st.error("Streamlit Secrets nincs beállítva.")
    st.stop()
elif status != "ok":
    st.error(f"Hitelesítési hiba: {status}")
    with st.expander("Hibaelhárítás"):
        st.markdown("""
**Leggyakoribb okok:**
1. **Domain-wide Delegation** nincs beállítva az Admin Console-ban, vagy rossz `client_id`-val
2. **Gmail API** nincs engedélyezve a Google Cloud projektben
3. **Secrets formátum** hibás – a `private_key` sorban a `\\n` karakterek nem alakultak át sortöréssé

**Ellenőrzési lépések:**
- admin.google.com → Security → API controls → Domain-wide Delegation → legyen benne: `109895706710901273647`
- console.cloud.google.com → APIs & Services → Gmail API → Enabled státusz
- Streamlit Secrets → a `private_key` értéke `"-----BEGIN PRIVATE KEY-----\\n...` formátumú legyen
        """)
    st.stop()


# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.header("⚙️ Beállítások")
    st.success(f"✓ Kapcsolódva: **{DELEGATED_EMAIL}**")
    mode = st.radio("Időszak", ["Aktuális hét", "Előző hét", "Aktuális hónap", "Egyéni"])
    now  = datetime.now(timezone.utc)

    if mode == "Aktuális hét":
        start = (now - timedelta(days=now.weekday())).replace(hour=0,minute=0,second=0,microsecond=0)
        end   = now
    elif mode == "Előző hét":
        start = (now - timedelta(days=now.weekday()+7)).replace(hour=0,minute=0,second=0,microsecond=0)
        end   = start + timedelta(days=7)
    elif mode == "Aktuális hónap":
        start = now.replace(day=1,hour=0,minute=0,second=0,microsecond=0)
        end   = now
    else:
        c1, c2 = st.columns(2)
        with c1: d1 = st.date_input("Kezdet", value=(now-timedelta(days=7)).date())
        with c2: d2 = st.date_input("Vég", value=now.date())
        start = datetime.combine(d1, datetime.min.time(), tzinfo=timezone.utc)
        end   = datetime.combine(d2, datetime.max.time(), tzinfo=timezone.utc)

    max_msgs = st.slider("Max. levelek", 100, 2000, 500, 100)
    run_btn  = st.button("🔄 Összesítés futtatása", use_container_width=True)

st.title("📧 Email forgalom összesítő")
st.caption(f"**{DELEGATED_EMAIL}** · {start.strftime('%Y. %m. %d.')} – {end.strftime('%Y. %m. %d.')}")

if run_btn:
    inbox, sent = fetch_period_data(service, start, end, max_msgs)
    stats = analyze(inbox, sent)
    st.session_state["result"] = {"inbox":inbox,"sent":sent,"stats":stats,"start":start,"end":end}

if "result" not in st.session_state:
    st.info("Válaszd ki az időszakot, majd kattints az **Összesítés futtatása** gombra.")
    st.stop()

res, inbox, sent, stats = (st.session_state["result"], st.session_state["result"]["inbox"],
                            st.session_state["result"]["sent"], st.session_state["result"]["stats"])

c1,c2,c3,c4 = st.columns(4)
for col,label,val,unit in [
    (c1,"📥 Beérkezett",len(inbox),"levél"),
    (c2,"📤 Küldött",len(sent),"levél"),
    (c3,"⏳ Válaszolatlan",len(stats["unanswered"]),"levél"),
    (c4,"⚡ Átl. válaszidő",fmt_hours(stats["avg_resp_h"]),""),
]:
    with col:
        st.markdown(f"""<div class="metric-card">
          <div class="metric-label">{label}</div>
          <div class="metric-value">{val}</div>
          <div class="metric-sub">{unit}</div></div>""", unsafe_allow_html=True)

st.markdown("---")
left, right = st.columns(2)

with left:
    st.markdown('<div class="section-title">👤 Top feladók</div>', unsafe_allow_html=True)
    if stats["top_senders"]:
        mc = stats["top_senders"][0][1]
        for item in stats["top_senders"]:
            st.markdown(bar(item, mc, "#4361ee"), unsafe_allow_html=True)
    st.markdown('<div class="section-title">⏱ Válaszidő</div>', unsafe_allow_html=True)
    r1,r2,r3 = st.columns(3)
    r1.metric("Átlagos", fmt_hours(stats["avg_resp_h"]))
    r2.metric("Medián",  fmt_hours(stats["med_resp_h"]))
    r3.metric("Mért",    stats["resp_count"])

with right:
    st.markdown('<div class="section-title">📋 Leggyakoribb témák</div>', unsafe_allow_html=True)
    if stats["top_subjects"]:
        mc = stats["top_subjects"][0][1]
        for item in stats["top_subjects"]:
            st.markdown(bar(item, mc, "#7209b7"), unsafe_allow_html=True)

st.markdown('<div class="section-title">⏳ Válaszolatlan levelek</div>', unsafe_allow_html=True)
if stats["unanswered"]:
    import pandas as pd
    rows = [{"Dátum": m["date"].strftime("%Y-%m-%d %H:%M") if m["date"] else "—",
             "Feladó": extract_email_addr(m["from"]), "Tárgy": m["subject"][:90]}
            for m in sorted(stats["unanswered"],
                key=lambda x: x["date"] or datetime.min.replace(tzinfo=timezone.utc),
                reverse=True)[:100]]
    st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)
else:
    st.success("Nincs válaszolatlan levél ebben az időszakban.")

st.markdown("---")
if st.button("📥 JSON export"):
    export = {"account": DELEGATED_EMAIL,
              "period": {"start": res["start"].isoformat(), "end": res["end"].isoformat()},
              "inbox_count": len(inbox), "sent_count": len(sent),
              "unanswered_count": len(stats["unanswered"]),
              "avg_response_hours": stats["avg_resp_h"],
              "top_senders": stats["top_senders"], "top_subjects": stats["top_subjects"]}
    b64 = base64.b64encode(json.dumps(export, ensure_ascii=False, indent=2).encode()).decode()
    fn  = f"email_summary_{res['start'].strftime('%Y%m%d')}.json"
    st.markdown(f'<a href="data:application/json;base64,{b64}" download="{fn}">⬇️ Letöltés</a>',
                unsafe_allow_html=True)
