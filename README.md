# Dashboard App

Basit kullanıcı adı/şifre ile giriş ve dashboard sayfası.

## Kurulum (Postgres)

```bash
npm install
npm run dev
```

Uygulama: `http://localhost:3000`

### Gerekli Ortam Değişkenleri

- `DATABASE_URL` (örnek: `postgres://user:pass@localhost:5432/dashboard`)
- `SESSION_SECRET`
- `SLACK_ANALYSIS_AUTO_SAVE_TIME` (opsiyonel, varsayılan: `23:59`, format: `HH:MM`)
  - Sunucu uyku/kapalı kalırsa açıldığında kaçırılan günler otomatik yakalanıp SQL'e kaydedilir.

### Opsiyonel Jira Analiz Ortam Değişkenleri

- `JIRA_BASE_URL` (örnek: `https://firma.atlassian.net`)
- `JIRA_EMAIL`
- `JIRA_API_TOKEN`
- `JIRA_API_TIMEOUT_MS` (varsayılan: `20000`)
- `JIRA_MAX_RESULTS` (varsayılan: `50`, max `200`)

## Ekranlar

- Dashboard: `/dashboard`
- Jira Analiz: `/reports/jira-analysis`
- Kullanıcılar: `/users`
- Ekranlar: `/screens`
- Yetkiler: `/permissions/:userId`

## İlk Kullanıcı

- Kullanıcı adı: `admin`
- Şifre: `admin123`

> İlk çalıştırmada veritabanı otomatik oluşur ve örnek kullanıcı eklenir.

## Render Deploy (Free + Postgres)

1) Projeyi GitHub'a push edin.  
2) Render'da **PostgreSQL** oluşturun.  
3) Render'da **Web Service** oluşturun (repo seçin).  
   - Build: `npm install`  
   - Start: `npm start`  
4) Environment variables ekleyin:
   - `DATABASE_URL` (Render Postgres bağlantı adresi)
   - `SESSION_SECRET` (rastgele güçlü bir değer)
   - `NODE_ENV=production`

Deploy sonrası Render size public URL verir.
