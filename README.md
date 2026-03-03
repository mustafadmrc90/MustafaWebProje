# Dashboard App

Basit kullanıcı adı/şifre ile giriş ve dashboard sayfası.

## Veritabani Desteği

- PostgreSQL (onerilen, Render ile uyumlu, ucretsiz planla Neon/Supabase kullanabilirsiniz)
- MSSQL (mevcut kurulumlarla geriye uyumlu)
- Uygulama `DATABASE_URL` formatina bakarak DB turunu otomatik secer.

## Kurulum (Yerel)

```bash
npm install
npm run dev
```

Uygulama: `http://localhost:3000`

`.env` desteği vardır. Hızlı başlangıç:

```bash
cp .env.example .env
```

### Gerekli Ortam Değişkenleri

- `DATABASE_URL`
  - PostgreSQL örnek: `postgresql://neondb_owner:***@ep-your-db-pooler.eu-central-1.aws.neon.tech/neondb?sslmode=require`
  - MSSQL örnek: `Server=18.159.75.68,1433;Database=obilet-b2b-preprod;User Id=corp_mdemirci;Password=***;Encrypt=True;TrustServerCertificate=True;`
- `DATABASE_SSL` (opsiyonel; URL formatinda etkili)
- `DATABASE_SSL_REJECT_UNAUTHORIZED` (opsiyonel; URL formatinda etkili)
- `SESSION_SECRET`
- `SLACK_BOT_TOKEN` (Slack analiz için gerekli)
- `SLACK_ANALYSIS_AUTO_SAVE_TIME` (opsiyonel, varsayılan: `23:59`, format: `HH:MM`)
  - Sunucu uyku/kapalı kalırsa açıldığında kaçırılan günler otomatik yakalanıp SQL'e kaydedilir.
- `SLACK_CORP_REQUEST_TAG` (opsiyonel, varsayılan: `@corpproduct`)
- `SLACK_CORP_REQUEST_CHANNELS` (opsiyonel, virgül ile kanal adı/id listesi; boşsa tüm taranan kanallar)

### Opsiyonel Tüm Firmalar / ObusMerkezSubeID Ortam Değişkenleri

- `INVENTORY_BRANCHES_API_URL` (varsayılan: `https://api-coreprod-cluster4.obus.com.tr/api/inventory/getbranches`)
- `INVENTORY_BRANCHES_API_AUTH` (varsayılan: `Basic MTIzNDU2MHg2NTUwR21STG5QYXJ5bnVt`)
- `INVENTORY_BRANCHES_LOGIN_USERNAME` (`ObusMerkezSubeID` doldurmak için gerekli)
- `INVENTORY_BRANCHES_LOGIN_PASSWORD` (`ObusMerkezSubeID` doldurmak için gerekli)
- `ALL_COMPANIES_FETCH_TIMEOUT_MS` (varsayılan: `180000`)
- `INVENTORY_BRANCHES_CLUSTER_CONCURRENCY` (varsayılan: `4`)

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

## Render Deploy (Web Service + Ucretsiz PostgreSQL)

1. Neon'da ucretsiz bir PostgreSQL proje olusturun ve `Connection string` (pooled) bilgisini alin.
2. Projeyi GitHub'a push edin.
3. Render'da **Web Service** olusturun (repo secin).
   - Build: `npm install`
   - Start: `npm start`
4. Render environment variables ekleyin:
   - `DATABASE_URL=postgresql://...?...sslmode=require`
   - `SESSION_SECRET` (rastgele guclu bir deger)
   - `NODE_ENV=production`
   - `DATABASE_SSL=true` (opsiyonel)
   - `DATABASE_SSL_REJECT_UNAUTHORIZED=false` (opsiyonel)
5. Deploy edin. Ilk acilista schema ve admin kullanicisi otomatik olusur.

Deploy sonrası Render size public URL verir.

## Veritabani Ilklendirme (Opsiyonel)

Sadece DB init calistirmak isterseniz:

```bash
./scripts/init-db-only.sh
```
