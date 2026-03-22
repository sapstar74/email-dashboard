# 📧 Email Dashboard – sales@deak.hu
## Telepítési útmutató (szerver, Service Account)

---

## Architektúra

```
Szerver (Linux/Windows)
  └── Streamlit app (app.py)
        └── Google Gmail API
              └── Service Account + Domain-wide Delegation
                    └── sales@deak.hu postaláda (olvasási jog)
```

---

## 1. Google Cloud beállítás

### 1.1 Projekt létrehozása
1. Nyisd meg: https://console.cloud.google.com/
2. Felső sáv → projekt választó → **New Project** → Név: `deak-email-dashboard`

### 1.2 Gmail API engedélyezése
1. **APIs & Services → Library** → keress rá: `Gmail API` → **Enable**

### 1.3 Service Account létrehozása
1. **APIs & Services → Credentials → + Create Credentials → Service Account**
2. Név: `email-dashboard-sa` → **Done**
3. Kattints a new service account sorára → **Keys** fül → **Add Key → JSON** → letöltés
4. Mentsd `service_account.json` névvel az `email_dashboard/` mappába

### 1.4 Client ID kiolvasása
Nyisd meg a `service_account.json` fájlt, jegyezd fel a `client_id` értékét.

---

## 2. Google Workspace Admin beállítás

1. Nyisd meg: https://admin.google.com/
2. **Security → Access and data control → API controls → Domain-wide Delegation → Add new**
   - Client ID: a `service_account.json`-ból a `client_id` értéke
   - OAuth Scopes: `https://www.googleapis.com/auth/gmail.readonly`
3. **Authorize**

> Csak olvasási jogot ad – a postaládában semmit sem módosít.

---

## 3. Telepítés

```bash
pip install -r requirements.txt
```

Opcionális `.env` fájl az app mellé:
```
SERVICE_ACCOUNT_FILE=service_account.json
DELEGATED_EMAIL=sales@deak.hu
```

---

## 4. Indítás

```bash
streamlit run app.py --server.port 8501 --server.address 0.0.0.0
```

### Systemd service (Linux, automatikus indítás)

`/etc/systemd/system/email-dashboard.service`:
```ini
[Unit]
Description=Email Dashboard
After=network.target

[Service]
WorkingDirectory=/opt/email_dashboard
ExecStart=/usr/local/bin/streamlit run app.py --server.port 8501 --server.address 0.0.0.0
Restart=always
Environment=DELEGATED_EMAIL=sales@deak.hu

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl enable --now email-dashboard
```

### Windows (NSSM)
```bat
nssm install EmailDashboard "streamlit.exe" "run app.py --server.port 8501"
nssm start EmailDashboard
```

---

## 5. Hibaelhárítás

| Hiba | Megoldás |
|------|----------|
| `service_account.json not found` | Másold a kulcsfájlt az app mellé |
| `403 Forbidden` | Domain-wide Delegation nincs az Admin Console-ban |
| `invalid_grant` | A delegált e-mail nem egyezik a Workspace doménnel |
| `Gmail API not enabled` | Cloud → APIs & Services → Gmail API engedélyezés |

> **Fontos**: `service_account.json` soha ne kerüljön publikus repository-ba!
> Adj hozzá `.gitignore` fájlt: `echo "service_account.json" > .gitignore`
