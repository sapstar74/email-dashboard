import streamlit as st
import json
import base64
import re
from datetime import datetime, timedelta, timezone
from collections import defaultdict
from email.utils import parsedate_to_datetime

from google.oauth2 import service_account
from googleapiclient.discovery import build
import pandas as pd
import plotly.graph_objects as go

DELEGATED_EMAIL = st.secrets.get("DELEGATED_EMAIL", "sales@deak.hu")
SCOPES          = ["https://www.googleapis.com/auth/gmail.readonly"]

st.set_page_config(page_title="Email Dashboard", page_icon="📧", layout="wide")

st.markdown("""
<style>
.metric-card { background:#f8f9fa; border-radius:12px; padding:16px 20px;
               text-align:center; border:1px solid #e9ecef; }
.metric-label { font-size:12px; color:#6c757d; margin-bottom:4px; }
.metric-value { font-size:28px; font-weight:600; color:#212529; }
.metric-sub   { font-size:11px; color:#adb5bd; margin-top:2px; }
.section-title { font-size:15px; font-weight:600; color:#495057;
                 margin:20px 0 10px; padding-bottom:6px; border-bottom:1px solid #e9ecef; }
</style>
""", unsafe_allow_html=True)


# ── Auth ──────────────────────────────────────────────────────────────────────
@st.cache_resource
def get_gmail_service():
    try:
        sa = dict(st.secrets["service_account"])
        pk = sa.get("private_key", "")
        if "\\n" in pk and "\n" not in pk:
            pk = pk.replace("\\n", "\n")
        if not pk.startswith("-----BEGIN PRIVATE KEY-----\n"):
            pk = pk.replace("-----BEGIN PRIVATE KEY-----", "-----BEGIN PRIVATE KEY-----\n")
        if "-----END PRIVATE KEY-----" in pk and not pk.endswith("\n-----END PRIVATE KEY-----\n"):
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


# ── Gmail helpers ─────────────────────────────────────────────────────────────
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

@st.cache_data(ttl=300, show_spinner=False)
def fetch_messages_cached(_service, query, max_results=500):
    messages, page_token = [], None
    while True:
        params = {"userId": "me", "q": query,
                  "maxResults": min(max_results - len(messages), 100)}
        if page_token:
            params["pageToken"] = page_token
        resp = _service.users().messages().list(**params).execute()
        messages.extend(resp.get("messages", []))
        page_token = resp.get("nextPageToken")
        if not page_token or len(messages) >= max_results:
            break
    return messages

@st.cache_data(ttl=300, show_spinner=False)
def get_message_meta_cached(_service, msg_id):
    msg = _service.users().messages().get(
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

@st.cache_data(ttl=600, show_spinner=False)
def get_label_map(_service):
    resp   = _service.users().labels().list(userId="me").execute()
    labels = resp.get("labels", [])
    result = {"(összes beérkező)": None}
    for l in labels:
        if l.get("type") == "user":
            result[l["name"]] = l["id"]
    return result

def fetch_period(service, start_dt, end_dt, max_results, label_id, label_name, prog_label):
    after  = int(start_dt.timestamp())
    before = int(end_dt.timestamp())

    if label_id:
        in_q = f"after:{after} before:{before} label:{label_id}"
    else:
        in_q = f"after:{after} before:{before} in:inbox"
    sent_q = f"after:{after} before:{before} in:sent"

    in_ids   = fetch_messages_cached(service, in_q,   max_results)
    sent_ids = fetch_messages_cached(service, sent_q, max_results)

    inbox, sent, done = [], [], 0
    total = len(in_ids) + len(sent_ids)
    prog  = st.progress(0, text=f"{prog_label} – 0/{total}")

    for m in in_ids:
        inbox.append(get_message_meta_cached(service, m["id"]))
        done += 1
        prog.progress(done / max(total, 1), text=f"{prog_label} – {done}/{total}")
    for m in sent_ids:
        sent.append(get_message_meta_cached(service, m["id"]))
        done += 1
        prog.progress(done / max(total, 1), text=f"{prog_label} – {done}/{total}")
    prog.empty()
    return inbox, sent


# ── Analitika ─────────────────────────────────────────────────────────────────
def extract_addr(raw):
    m = re.search(r"<([^>]+)>", raw)
    return m.group(1).lower() if m else raw.strip().lower()

def analyze(inbox, sent):
    sent_threads = {m["threadId"] for m in sent}
    unanswered   = [m for m in inbox if m["threadId"] not in sent_threads]

    sender_count = defaultdict(int)
    for m in inbox:
        sender_count[extract_addr(m["from"])] += 1
    top_senders = sorted(sender_count.items(), key=lambda x: -x[1])[:15]

    subj_count = defaultdict(int)
    for m in inbox:
        s = re.sub(r"^(re:|fwd?:|aw:)\s*", "", m["subject"], flags=re.IGNORECASE).strip()
        subj_count[" ".join(s.split()[:5]) or "(nincs tárgy)"] += 1
    top_subjects = sorted(subj_count.items(), key=lambda x: -x[1])[:15]

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

    avg = sum(resp_times) / len(resp_times) if resp_times else None
    med = sorted(resp_times)[len(resp_times) // 2] if resp_times else None
    return {
        "unanswered":   unanswered,
        "top_senders":  top_senders,
        "top_subjects": top_subjects,
        "avg_resp_h":   avg,
        "med_resp_h":   med,
        "resp_count":   len(resp_times),
        "inbox_count":  len(inbox),
        "sent_count":   len(sent),
    }

def daily_counts(inbox, sent, start_dt, end_dt):
    days = {}
    cur = start_dt.date()
    while cur <= end_dt.date():
        days[cur] = {"inbox": 0, "sent": 0}
        cur += timedelta(days=1)
    for m in inbox:
        if m["date"]:
            d = m["date"].date()
            if d in days: days[d]["inbox"] += 1
    for m in sent:
        if m["date"]:
            d = m["date"].date()
            if d in days: days[d]["sent"] += 1
    return days

def fmt_hours(h):
    if h is None: return "—"
    if h < 1:     return f"{int(h*60)} perc"
    if h < 24:    return f"{h:.1f} óra"
    return f"{h/24:.1f} nap"

def delta_html(v1, v2, lower_is_better=False):
    if not v2: return ""
    diff = v1 - v2
    if diff == 0: return '<div style="font-size:12px;color:#6c757d">= változatlan</div>'
    pct  = abs(diff / v2 * 100)
    pos  = diff > 0
    if lower_is_better: pos = not pos
    color = "#198754" if pos else "#dc3545"
    arrow = "▲" if diff > 0 else "▼"
    return f'<div style="font-size:12px;color:{color}">{arrow} {abs(diff):.0f} ({pct:.0f}%)</div>'

def bar_row(label, cnt, max_c, color):
    return f"""<div style="margin-bottom:8px;">
      <div style="display:flex;justify-content:space-between;font-size:13px;margin-bottom:3px;">
        <span style="color:#495057;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;max-width:80%">{label}</span>
        <span style="color:#6c757d;font-weight:600">{cnt}</span>
      </div>
      <div style="background:#e9ecef;border-radius:4px;height:5px;">
        <div style="background:{color};width:{cnt/max_c*100:.0f}%;height:5px;border-radius:4px;"></div>
      </div></div>"""

def preset_dates(mode, now):
    if mode == "Aktuális hét":
        s = (now - timedelta(days=now.weekday())).replace(hour=0,minute=0,second=0,microsecond=0)
        return s, now
    if mode == "Előző hét":
        s = (now - timedelta(days=now.weekday()+7)).replace(hour=0,minute=0,second=0,microsecond=0)
        return s, s + timedelta(days=7)
    if mode == "Aktuális hónap":
        return now.replace(day=1,hour=0,minute=0,second=0,microsecond=0), now
    if mode == "Előző hónap":
        first = now.replace(day=1,hour=0,minute=0,second=0,microsecond=0)
        last  = first - timedelta(seconds=1)
        return last.replace(day=1,hour=0,minute=0,second=0,microsecond=0), first
    return None, None


# ── Auth ellenőrzés ───────────────────────────────────────────────────────────
service, status = get_gmail_service()

if status == "secrets_missing":
    st.error("Streamlit Secrets nincs beállítva.")
    st.stop()
elif status != "ok":
    st.error(f"Hitelesítési hiba: {status}")
    with st.expander("Hibaelhárítás"):
        st.markdown("""
- Domain-wide Delegation beállítva? → **admin.google.com** → Security → API controls
- Gmail API engedélyezve? → **console.cloud.google.com** → APIs & Services
- `private_key` formátuma helyes? Futtasd a `convert.py` szkriptet
        """)
    st.stop()

label_map = get_label_map(service)


# ── Sidebar ───────────────────────────────────────────────────────────────────
now = datetime.now(timezone.utc)

with st.sidebar:
    st.header("⚙️ Beállítások")
    st.success(f"✓ {DELEGATED_EMAIL}")

    max_msgs = st.slider("Max. levelek", 100, 2000, 500, 100)

    st.markdown("---")
    st.subheader("📁 Mappa")
    sel_label_name = st.selectbox("Mappa szűrő", list(label_map.keys()))
    sel_label_id   = label_map[sel_label_name]

    st.markdown("---")
    st.subheader("📅 Időszak 1")
    PRESETS = ["Aktuális hét", "Előző hét", "Aktuális hónap", "Előző hónap", "Egyéni"]
    mode1 = st.selectbox("", PRESETS, key="mode1")
    if mode1 == "Egyéni":
        c1, c2 = st.columns(2)
        with c1: d1s = st.date_input("Kezdet", (now-timedelta(days=7)).date(), key="d1s")
        with c2: d1e = st.date_input("Vég",    now.date(), key="d1e")
        start1 = datetime.combine(d1s, datetime.min.time(), tzinfo=timezone.utc)
        end1   = datetime.combine(d1e, datetime.max.time(), tzinfo=timezone.utc)
    else:
        start1, end1 = preset_dates(mode1, now)

    st.markdown("---")
    compare = st.checkbox("🔄 Összehasonlítás bekapcsolása")
    start2 = end2 = None
    if compare:
        st.subheader("📅 Időszak 2")
        mode2 = st.selectbox("", PRESETS, index=1, key="mode2")
        if mode2 == "Egyéni":
            c1, c2 = st.columns(2)
            with c1: d2s = st.date_input("Kezdet", (now-timedelta(days=14)).date(), key="d2s")
            with c2: d2e = st.date_input("Vég",    (now-timedelta(days=7)).date(),  key="d2e")
            start2 = datetime.combine(d2s, datetime.min.time(), tzinfo=timezone.utc)
            end2   = datetime.combine(d2e, datetime.max.time(), tzinfo=timezone.utc)
        else:
            start2, end2 = preset_dates(mode2, now)

    st.markdown("---")
    run_btn = st.button("🔄 Összesítés futtatása", use_container_width=True)


# ── Fejléc ────────────────────────────────────────────────────────────────────
st.title("📧 Email forgalom összesítő")
st.caption(f"**{DELEGATED_EMAIL}** · mappa: **{sel_label_name}**")

if run_btn:
    inbox1, sent1 = fetch_period(service, start1, end1, max_msgs,
                                 sel_label_id, sel_label_name, "1. időszak")
    stats1 = analyze(inbox1, sent1)
    daily1 = daily_counts(inbox1, sent1, start1, end1)

    result = {"inbox1": inbox1, "sent1": sent1, "stats1": stats1, "daily1": daily1,
              "start1": start1, "end1": end1, "label": sel_label_name}

    if compare and start2 and end2:
        inbox2, sent2 = fetch_period(service, start2, end2, max_msgs,
                                     sel_label_id, sel_label_name, "2. időszak")
        stats2 = analyze(inbox2, sent2)
        daily2 = daily_counts(inbox2, sent2, start2, end2)
        result.update({"inbox2": inbox2, "sent2": sent2, "stats2": stats2,
                       "daily2": daily2, "start2": start2, "end2": end2})

    st.session_state["result"] = result

if "result" not in st.session_state:
    st.info("Válaszd ki a beállításokat, majd kattints az **Összesítés futtatása** gombra.")
    st.stop()

res    = st.session_state["result"]
stats1 = res["stats1"]
stats2 = res.get("stats2")
s1, e1 = res["start1"], res["end1"]
s2, e2 = res.get("start2"), res.get("end2")
lbl1   = f"{s1.strftime('%m.%d')}–{e1.strftime('%m.%d')}"
lbl2   = f"{s2.strftime('%m.%d')}–{e2.strftime('%m.%d')}" if s2 else None

tab_names = ["📊 Összesítés", "📈 Tendencia"]
if stats2: tab_names.insert(1, "🔄 Összehasonlítás")
tabs = st.tabs(tab_names)
tidx = {n: i for i, n in enumerate(tab_names)}


# ═══════════════════════════════════════════════════════════════════════════════
# TAB – Összesítés
# ═══════════════════════════════════════════════════════════════════════════════
with tabs[tidx["📊 Összesítés"]]:
    st.caption(f"**{lbl1}** · {res['label']}")
    c1,c2,c3,c4 = st.columns(4)
    for col, lbl, val, unit in [
        (c1,"📥 Beérkezett",    stats1["inbox_count"],          "levél"),
        (c2,"📤 Küldött",       stats1["sent_count"],           "levél"),
        (c3,"⏳ Válaszolatlan", len(stats1["unanswered"]),      "levél"),
        (c4,"⚡ Válaszidő",     fmt_hours(stats1["avg_resp_h"]),""),
    ]:
        with col:
            st.markdown(f"""<div class="metric-card">
              <div class="metric-label">{lbl}</div>
              <div class="metric-value">{val}</div>
              <div class="metric-sub">{unit}</div></div>""", unsafe_allow_html=True)

    st.markdown("---")
    left, right = st.columns(2)

    with left:
        st.markdown('<div class="section-title">👤 Top feladók</div>', unsafe_allow_html=True)
        if stats1["top_senders"]:
            mc = stats1["top_senders"][0][1]
            for addr, cnt in stats1["top_senders"]:
                st.markdown(bar_row(addr, cnt, mc, "#4361ee"), unsafe_allow_html=True)
        st.markdown('<div class="section-title">⏱ Válaszidő részletek</div>', unsafe_allow_html=True)
        r1,r2,r3 = st.columns(3)
        r1.metric("Átlagos", fmt_hours(stats1["avg_resp_h"]))
        r2.metric("Medián",  fmt_hours(stats1["med_resp_h"]))
        r3.metric("Mért szálak", stats1["resp_count"])

    with right:
        st.markdown('<div class="section-title">📋 Leggyakoribb témák</div>', unsafe_allow_html=True)
        if stats1["top_subjects"]:
            mc = stats1["top_subjects"][0][1]
            for subj, cnt in stats1["top_subjects"]:
                st.markdown(bar_row(subj, cnt, mc, "#7209b7"), unsafe_allow_html=True)

    st.markdown('<div class="section-title">⏳ Válaszolatlan levelek</div>', unsafe_allow_html=True)
    if stats1["unanswered"]:
        rows = [{"Dátum":  m["date"].strftime("%Y-%m-%d %H:%M") if m["date"] else "—",
                 "Feladó": extract_addr(m["from"]), "Tárgy": m["subject"][:90]}
                for m in sorted(stats1["unanswered"],
                    key=lambda x: x["date"] or datetime.min.replace(tzinfo=timezone.utc),
                    reverse=True)[:100]]
        st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)
    else:
        st.success("Nincs válaszolatlan levél.")


# ═══════════════════════════════════════════════════════════════════════════════
# TAB – Összehasonlítás
# ═══════════════════════════════════════════════════════════════════════════════
if stats2 and "🔄 Összehasonlítás" in tidx:
    with tabs[tidx["🔄 Összehasonlítás"]]:
        st.caption(f"**{lbl1}** vs **{lbl2}** · {res['label']}")

        c1,c2,c3,c4 = st.columns(4)
        metrics = [
            (c1,"📥 Beérkezett",    stats1["inbox_count"],        stats2["inbox_count"],        False),
            (c2,"📤 Küldött",       stats1["sent_count"],         stats2["sent_count"],          False),
            (c3,"⏳ Válaszolatlan", len(stats1["unanswered"]),    len(stats2["unanswered"]),     True),
            (c4,"⚡ Válaszidő",     stats1["avg_resp_h"] or 0,   stats2["avg_resp_h"] or 0,     True),
        ]
        for col, lbl, v1, v2, lib in metrics:
            with col:
                disp = fmt_hours(v1) if "Válaszidő" in lbl else v1
                disp2 = fmt_hours(v2) if "Válaszidő" in lbl else v2
                st.markdown(f"""<div class="metric-card">
                  <div class="metric-label">{lbl}</div>
                  <div class="metric-value">{disp}</div>
                  <div class="metric-sub">{lbl1}: {disp} · {lbl2}: {disp2}</div>
                  {delta_html(v1, v2, lib)}</div>""", unsafe_allow_html=True)

        st.markdown("---")

        # Feladók összehasonlítás
        st.markdown('<div class="section-title">👤 Top feladók összehasonlítása</div>', unsafe_allow_html=True)
        s1m = dict(stats1["top_senders"]); s2m = dict(stats2["top_senders"])
        all_s = sorted(set(s1m)|set(s2m), key=lambda x: -(s1m.get(x,0)+s2m.get(x,0)))[:12]
        fig = go.Figure([
            go.Bar(name=lbl1, x=all_s, y=[s1m.get(s,0) for s in all_s], marker_color="#4361ee"),
            go.Bar(name=lbl2, x=all_s, y=[s2m.get(s,0) for s in all_s], marker_color="#adb5bd"),
        ])
        fig.update_layout(barmode="group", height=300, margin=dict(l=0,r=0,t=10,b=80),
                          xaxis_tickangle=-30, legend=dict(orientation="h",y=1.12))
        st.plotly_chart(fig, use_container_width=True)

        # Témák összehasonlítás
        st.markdown('<div class="section-title">📋 Leggyakoribb témák összehasonlítása</div>', unsafe_allow_html=True)
        t1m = dict(stats1["top_subjects"]); t2m = dict(stats2["top_subjects"])
        all_t = sorted(set(t1m)|set(t2m), key=lambda x: -(t1m.get(x,0)+t2m.get(x,0)))[:12]
        fig2 = go.Figure([
            go.Bar(name=lbl1, x=all_t, y=[t1m.get(s,0) for s in all_t], marker_color="#7209b7"),
            go.Bar(name=lbl2, x=all_t, y=[t2m.get(s,0) for s in all_t], marker_color="#d0b4f0"),
        ])
        fig2.update_layout(barmode="group", height=300, margin=dict(l=0,r=0,t=10,b=100),
                           xaxis_tickangle=-35, legend=dict(orientation="h",y=1.12))
        st.plotly_chart(fig2, use_container_width=True)


# ═══════════════════════════════════════════════════════════════════════════════
# TAB – Tendencia
# ═══════════════════════════════════════════════════════════════════════════════
with tabs[tidx["📈 Tendencia"]]:
    st.caption(f"Napi/heti forgalom · {res['label']}")
    daily1 = res.get("daily1", {})
    daily2 = res.get("daily2", {})

    if not daily1:
        st.info("Futtasd az összesítést az adatok megjelenítéséhez.")
    else:
        gran = st.radio("Bontás", ["Napi", "Heti"], horizontal=True)

        def aggregate(daily, gran):
            if gran == "Napi":
                return {str(d): v for d, v in sorted(daily.items())}
            weekly = defaultdict(lambda: {"inbox": 0, "sent": 0})
            for d, v in daily.items():
                wk = str(d - timedelta(days=d.weekday()))
                weekly[wk]["inbox"] += v["inbox"]
                weekly[wk]["sent"]  += v["sent"]
            return dict(sorted(weekly.items()))

        agg1 = aggregate(daily1, gran)
        fig  = go.Figure()
        fig.add_trace(go.Scatter(x=list(agg1.keys()), y=[v["inbox"] for v in agg1.values()],
                                 name=f"Beérkezett – {lbl1}", mode="lines+markers",
                                 line=dict(color="#4361ee", width=2), marker=dict(size=5)))
        fig.add_trace(go.Scatter(x=list(agg1.keys()), y=[v["sent"] for v in agg1.values()],
                                 name=f"Küldött – {lbl1}", mode="lines+markers",
                                 line=dict(color="#4361ee", width=2, dash="dot"), marker=dict(size=5)))

        if daily2 and lbl2:
            agg2 = aggregate(daily2, gran)
            fig.add_trace(go.Scatter(x=list(agg2.keys()), y=[v["inbox"] for v in agg2.values()],
                                     name=f"Beérkezett – {lbl2}", mode="lines+markers",
                                     line=dict(color="#adb5bd", width=2), marker=dict(size=5)))
            fig.add_trace(go.Scatter(x=list(agg2.keys()), y=[v["sent"] for v in agg2.values()],
                                     name=f"Küldött – {lbl2}", mode="lines+markers",
                                     line=dict(color="#adb5bd", width=2, dash="dot"), marker=dict(size=5)))

        fig.update_layout(height=420, margin=dict(l=0,r=0,t=20,b=0),
                          legend=dict(orientation="h", y=-0.18),
                          xaxis=dict(title="Dátum", tickangle=-30),
                          yaxis=dict(title="Levelek száma"),
                          hovermode="x unified")
        st.plotly_chart(fig, use_container_width=True)

        # Napi átlagok
        st.markdown('<div class="section-title">📊 Napi átlagok</div>', unsafe_allow_html=True)
        avg_in1 = sum(v["inbox"] for v in daily1.values()) / max(len(daily1),1)
        avg_s1  = sum(v["sent"]  for v in daily1.values()) / max(len(daily1),1)
        ca,cb,cc,cd = st.columns(4)
        ca.metric(f"Beérkezett/nap ({lbl1})", f"{avg_in1:.1f}")
        cb.metric(f"Küldött/nap ({lbl1})",    f"{avg_s1:.1f}")
        if daily2:
            avg_in2 = sum(v["inbox"] for v in daily2.values()) / max(len(daily2),1)
            avg_s2  = sum(v["sent"]  for v in daily2.values()) / max(len(daily2),1)
            cc.metric(f"Beérkezett/nap ({lbl2})", f"{avg_in2:.1f}", delta=f"{avg_in1-avg_in2:+.1f}")
            cd.metric(f"Küldött/nap ({lbl2})",    f"{avg_s2:.1f}",  delta=f"{avg_s1-avg_s2:+.1f}")


# ── Export ────────────────────────────────────────────────────────────────────
st.markdown("---")
if st.button("📥 JSON export"):
    export = {"account": DELEGATED_EMAIL, "label": res["label"],
              "period1": {"start": res["start1"].isoformat(), "end": res["end1"].isoformat(),
                          "inbox": stats1["inbox_count"], "sent": stats1["sent_count"],
                          "unanswered": len(stats1["unanswered"]),
                          "avg_response_hours": stats1["avg_resp_h"],
                          "top_senders": stats1["top_senders"],
                          "top_subjects": stats1["top_subjects"]}}
    if stats2:
        export["period2"] = {"start": res["start2"].isoformat(), "end": res["end2"].isoformat(),
                             "inbox": stats2["inbox_count"], "sent": stats2["sent_count"],
                             "unanswered": len(stats2["unanswered"]),
                             "avg_response_hours": stats2["avg_resp_h"]}
    b64 = base64.b64encode(json.dumps(export, ensure_ascii=False, indent=2).encode()).decode()
    fn  = f"email_summary_{res['start1'].strftime('%Y%m%d')}.json"
    st.markdown(f'<a href="data:application/json;base64,{b64}" download="{fn}">⬇️ Letöltés</a>',
                unsafe_allow_html=True)
