import streamlit as st
import json
import base64
import re
import time
from datetime import datetime, timedelta, timezone
from collections import defaultdict
from email.utils import parsedate_to_datetime

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import pandas as pd
import plotly.graph_objects as go

DELEGATED_EMAIL = st.secrets.get("DELEGATED_EMAIL", "sales@deak.hu")
SCOPES          = ["https://www.googleapis.com/auth/gmail.readonly"]

st.set_page_config(page_title="Email Dashboard", page_icon="📧", layout="wide")

st.markdown("""
<style>
.metric-card { background:var(--color-background-secondary); border-radius:12px; padding:16px 20px;
               text-align:center; border:1px solid var(--color-border-tertiary); }
.metric-label { font-size:12px; color:var(--color-text-secondary); margin-bottom:4px; }
.metric-value { font-size:28px; font-weight:600; color:var(--color-text-primary); }
.metric-sub   { font-size:11px; color:var(--color-text-tertiary); margin-top:2px; }
.section-title { font-size:15px; font-weight:600; color:var(--color-text-primary);
                 margin:20px 0 10px; padding-bottom:6px; border-bottom:1px solid var(--color-border-tertiary); }
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

def api_call_with_retry(fn, retries=4, base_delay=2):
    """503/500 hibánál automatikusan újrapróbál, exponenciális várakozással."""
    for attempt in range(retries):
        try:
            return fn()
        except HttpError as e:
            if e.resp.status in (500, 503) and attempt < retries - 1:
                wait = base_delay * (2 ** attempt)  # 2, 4, 8, 16 mp
                time.sleep(wait)
            else:
                raise
        except Exception:
            raise

@st.cache_data(ttl=300, show_spinner=False)
def get_message_meta_cached(_service, msg_id):
    def _fetch():
        return _service.users().messages().get(
            userId="me", id=msg_id, format="metadata",
            metadataHeaders=["From", "To", "Subject", "Date"],
        ).execute()

    msg     = api_call_with_retry(_fetch)
    headers = msg.get("payload", {}).get("headers", [])
    return {
        "id":       msg_id,
        "from":     get_header(headers, "From"),
        "to":       get_header(headers, "To"),
        "subject":  get_header(headers, "Subject"),
        "date":     parse_date(get_header(headers, "Date")),
        "threadId": msg.get("threadId", ""),
    }

@st.cache_data(ttl=3600, show_spinner=False)
def get_message_body_cached(_service, msg_id):
    """Levél teljes tartalmának lekérése – csak kattintásra hívódik meg."""
    def _fetch():
        return _service.users().messages().get(
            userId="me", id=msg_id, format="full"
        ).execute()
    msg = api_call_with_retry(_fetch)

    def decode_part(part):
        data = part.get("body", {}).get("data", "")
        if data:
            import base64 as b64
            return b64.urlsafe_b64decode(data + "==").decode("utf-8", errors="replace")
        return ""

    def extract_text(payload):
        mime = payload.get("mimeType", "")
        if mime == "text/plain":
            return decode_part(payload), ""
        if mime == "text/html":
            return "", decode_part(payload)
        plain, html = "", ""
        for part in payload.get("parts", []):
            p, h = extract_text(part)
            plain += p; html += h
        return plain, html

    headers = msg.get("payload", {}).get("headers", [])
    plain, html = extract_text(msg.get("payload", {}))
    return {
        "from":    get_header(headers, "From"),
        "to":      get_header(headers, "To"),
        "subject": get_header(headers, "Subject"),
        "date":    get_header(headers, "Date"),
        "plain":   plain.strip(),
        "html":    html.strip(),
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
    """Statikus sáv – összehasonlítás tabban használt."""
    pct = round(cnt / max_c * 100)
    return f"""<div style="margin-bottom:8px;">
      <div style="display:flex;justify-content:space-between;font-size:13px;margin-bottom:3px;">
        <span style="color:#495057;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;max-width:80%">{label}</span>
        <span style="color:#6c757d;font-weight:600">{cnt}</span>
      </div>
      <div style="background:#e9ecef;border-radius:4px;height:5px;">
        <div style="background:{color};width:{pct}%;height:5px;border-radius:4px;"></div>
      </div></div>"""

def interactive_bar_chart(items, color, chart_id, inbox_data):
    """Kattintható sávdiagram – kattintásra megjelennek a levelek."""
    if not items:
        return ""
    max_c = items[0][1]

    index = defaultdict(list)
    for m in inbox_data:
        addr = extract_addr(m["from"])
        index[addr].append(m)
        s = re.sub(r"^(re:|fwd?:|aw:)\s*", "", m["subject"], flags=re.IGNORECASE).strip()
        key = " ".join(s.split()[:5]) or "(nincs tárgy)"
        if key not in index or m not in index[key]:
            index[key].append(m)

    chart_data = []
    for label, cnt in items:
        msgs = index.get(label, [])
        chart_data.append({
            "label": label,
            "count": cnt,
            "pct":   round(cnt / max_c * 100),
            "msgs":  [{"date": m["date"].strftime("%Y-%m-%d %H:%M") if m["date"] else "—",
                       "from": m["from"],
                       "subj": m["subject"][:120],
                       "id":   m["id"]} for m in sorted(msgs,
                           key=lambda x: x["date"] or datetime.min.replace(tzinfo=timezone.utc),
                           reverse=True)[:30]]
        })

    data_json = json.dumps(chart_data, ensure_ascii=False)

    return f"""
<div id="{chart_id}_wrap" style="font-family:sans-serif;">
  <div id="{chart_id}_bars"></div>
  <div id="{chart_id}_panel" style="display:none;margin-top:12px;border:1px solid #dee2e6;
       border-radius:8px;overflow:hidden;">
    <div style="background:#f8f9fa;padding:10px 14px;display:flex;justify-content:space-between;
         align-items:center;border-bottom:1px solid #dee2e6;">
      <span id="{chart_id}_panel_title" style="font-weight:600;font-size:13px;color:#212529;"></span>
      <button onclick="document.getElementById(\'{chart_id}_panel\').style.display=\'none\'"
        style="border:none;background:none;font-size:18px;cursor:pointer;color:#6c757d;line-height:1;">x</button>
    </div>
    <div id="{chart_id}_panel_body" style="max-height:320px;overflow-y:auto;"></div>
  </div>
</div>
<script>
(function() {{
  var data  = {data_json};
  var color = "{color}";
  var cid   = "{chart_id}";
  var wrap  = document.getElementById(cid + "_bars");
  data.forEach(function(item) {{
    var row = document.createElement("div");
    row.style.cssText = "margin-bottom:8px;cursor:pointer;padding:4px 6px;border-radius:6px;transition:background .15s";
    row.onmouseover = function() {{ this.style.background="#f1f3f5"; }};
    row.onmouseout  = function() {{ this.style.background=""; }};
    row.innerHTML =
      "<div style=\"display:flex;justify-content:space-between;font-size:13px;margin-bottom:3px;\">" +
      "<span style=\"color:#495057;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;max-width:82%\">" + item.label + "</span>" +
      "<span style=\"color:#6c757d;font-weight:600\">" + item.count + "</span></div>" +
      "<div style=\"background:#e9ecef;border-radius:4px;height:6px;\">" +
      "<div style=\"background:" + color + ";width:" + item.pct + "%;height:6px;border-radius:4px;\"></div></div>";
    row.onclick = function() {{
      var panel = document.getElementById(cid + "_panel");
      var title = document.getElementById(cid + "_panel_title");
      var body  = document.getElementById(cid + "_panel_body");
      title.textContent = item.label + "  (" + item.count + " lev\u00e9l)";
      if (!item.msgs || item.msgs.length === 0) {{
        body.innerHTML = "<div style=\"padding:16px;color:#6c757d;font-size:13px;\">Nincs megjelen\u00edthet\u0151 lev\u00e9l.</div>";
      }} else {{
        body.innerHTML = item.msgs.map(function(m) {{
          return "<div style=\"padding:10px 14px;border-bottom:1px solid #f1f3f5;\">" +
            "<div style=\"font-size:11px;color:#adb5bd;margin-bottom:2px;\">" + m.date + "</div>" +
            "<div style=\"font-size:12px;color:#6c757d;margin-bottom:2px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;\">" + m.from + "</div>" +
            "<div style=\"font-size:13px;color:#212529;font-weight:500;\">" + m.subj + "</div></div>";
        }}).join("");
      }}
      panel.style.display = "block";
      panel.scrollIntoView({{behavior:"smooth", block:"nearest"}});
    }};
    wrap.appendChild(row);
  }});
}})();
</script>"""

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

    # Metric kártyák – Beérkezett és Küldött kattintható
    c1,c2,c3,c4 = st.columns(4)

    with c1:
        if st.button(f"📥 Beérkezett\n{stats1['inbox_count']} levél", use_container_width=True, key="btn_inbox"):
            st.session_state["msg_view"] = ("inbox", None)
    with c2:
        if st.button(f"📤 Küldött\n{stats1['sent_count']} levél", use_container_width=True, key="btn_sent"):
            st.session_state["msg_view"] = ("sent", None)
    with c3:
        st.markdown(f"""<div class="metric-card">
          <div class="metric-label">⏳ Válaszolatlan</div>
          <div class="metric-value">{len(stats1["unanswered"])}</div>
          <div class="metric-sub">levél</div></div>""", unsafe_allow_html=True)
    with c4:
        st.markdown(f"""<div class="metric-card">
          <div class="metric-label">⚡ Válaszidő</div>
          <div class="metric-value">{fmt_hours(stats1["avg_resp_h"])}</div>
          <div class="metric-sub"></div></div>""", unsafe_allow_html=True)

    # ── Levéllista panel ──────────────────────────────────────────────────────
    mv = st.session_state.get("msg_view")
    if mv:
        mv_type, mv_id = mv
        source = res["inbox1"] if mv_type == "inbox" else res["sent1"]
        label_str = "Beérkezett" if mv_type == "inbox" else "Küldött"

        with st.container():
            hdr, close_col = st.columns([8,1])
            with hdr:
                st.markdown(f'<div class="section-title">{"📥" if mv_type=="inbox" else "📤"} {label_str} levelek</div>', unsafe_allow_html=True)
            with close_col:
                if st.button("✕ Bezár", key="close_msglist"):
                    st.session_state.pop("msg_view", None)
                    st.rerun()

            if mv_id is None:
                # Levéllista – subject-ek
                sorted_msgs = sorted(source,
                    key=lambda x: x["date"] or datetime.min.replace(tzinfo=timezone.utc),
                    reverse=True)[:200]

                for i, m in enumerate(sorted_msgs):
                    date_str = m["date"].strftime("%Y-%m-%d %H:%M") if m["date"] else "—"
                    addr     = extract_addr(m["from"]) if mv_type=="inbox" else extract_addr(m.get("to",""))
                    subj     = m["subject"] or "(nincs tárgy)"
                    col_info, col_btn = st.columns([7,1])
                    with col_info:
                        st.markdown(f"""<div style="
                              padding:10px 12px;
                              margin-bottom:6px;
                              border-radius:8px;
                              border:1px solid var(--color-border-tertiary);
                              background:var(--color-background-secondary);">
                          <div style="font-size:11px;
                                      color:var(--color-text-tertiary);
                                      margin-bottom:3px;">
                            {date_str} &nbsp;·&nbsp; {addr}
                          </div>
                          <div style="font-size:14px;
                                      font-weight:500;
                                      color:var(--color-text-primary);
                                      line-height:1.4;">
                            {subj[:100]}
                          </div>
                        </div>""", unsafe_allow_html=True)
                    with col_btn:
                        if st.button("📖", key=f"open_{m['id']}", help="Levél megnyitása"):
                            st.session_state["msg_view"] = (mv_type, m["id"])
                            st.rerun()
            else:
                # Levél teljes tartalma
                if st.button("← Vissza a listához", key="back_to_list"):
                    st.session_state["msg_view"] = (mv_type, None)
                    st.rerun()

                with st.spinner("Levél betöltése…"):
                    body = get_message_body_cached(service, mv_id)

                st.markdown(f"""<div style="background:var(--color-background-secondary);
                    border-radius:8px;padding:14px 18px;margin-bottom:12px;
                    border:1px solid var(--color-border-tertiary);">
                  <div style="font-size:12px;color:var(--color-text-tertiary);margin-bottom:6px">{body['date']}</div>
                  <div style="font-size:13px;color:var(--color-text-secondary);margin-bottom:2px">
                    <span style="color:var(--color-text-tertiary)">Feladó:</span> {body['from']}</div>
                  <div style="font-size:13px;color:var(--color-text-secondary);margin-bottom:8px">
                    <span style="color:var(--color-text-tertiary)">Címzett:</span> {body['to']}</div>
                  <div style="font-size:16px;font-weight:600;color:var(--color-text-primary)">{body['subject']}</div>
                </div>""", unsafe_allow_html=True)

                if body["html"]:
                    st.components.v1.html(
                        f"""<div style="font-family:sans-serif;font-size:14px;
                            line-height:1.6;color:#212529;padding:4px">
                          {body["html"]}
                        </div>""",
                        height=600, scrolling=True
                    )
                elif body["plain"]:
                    st.text_area("Tartalom", body["plain"], height=400)
                else:
                    st.info("A levél tartalma nem jeleníthető meg.")
        st.markdown("---")

    st.markdown("---")
    left, right = st.columns(2)

    with left:
        st.markdown('<div class="section-title">👤 Top feladók <small style="font-weight:400;color:#adb5bd">– kattints egy sávra a levelekért</small></div>', unsafe_allow_html=True)
        if stats1["top_senders"]:
            st.components.v1.html(
                interactive_bar_chart(stats1["top_senders"], "#4361ee", "senders", res["inbox1"]),
                height=len(stats1["top_senders"]) * 36 + 20, scrolling=False
            )
        st.markdown('<div class="section-title">⏱ Válaszidő részletek</div>', unsafe_allow_html=True)
        r1,r2,r3 = st.columns(3)
        r1.metric("Átlagos", fmt_hours(stats1["avg_resp_h"]))
        r2.metric("Medián",  fmt_hours(stats1["med_resp_h"]))
        r3.metric("Mért szálak", stats1["resp_count"])

    with right:
        st.markdown('<div class="section-title">📋 Leggyakoribb témák <small style="font-weight:400;color:#adb5bd">– kattints egy sávra a levelekért</small></div>', unsafe_allow_html=True)
        if stats1["top_subjects"]:
            st.components.v1.html(
                interactive_bar_chart(stats1["top_subjects"], "#7209b7", "subjects", res["inbox1"]),
                height=len(stats1["top_subjects"]) * 36 + 20, scrolling=False
            )

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
