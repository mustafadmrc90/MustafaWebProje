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

## Ekranlar

- Dashboard: `/dashboard`
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
