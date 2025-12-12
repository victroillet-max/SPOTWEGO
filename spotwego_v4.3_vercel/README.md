# Spotwego V4.3 - Vercel Deployment

## Quick Deploy

### Option 1: Vercel CLI

1. Install Vercel CLI:
   ```bash
   npm install -g vercel
   ```

2. Login to Vercel:
   ```bash
   vercel login
   ```

3. Deploy:
   ```bash
   cd spotwego_vercel
   vercel
   ```

4. Add environment variables in Vercel Dashboard

### Option 2: GitHub Integration

1. Push this folder to a GitHub repo
2. Go to https://vercel.com/new
3. Import your GitHub repo
4. Vercel will auto-detect the Python app
5. Add environment variables before deploying

## Environment Variables (Required)

Add these in Vercel Dashboard → Your Project → Settings → Environment Variables:

| Variable | Value |
|----------|-------|
| `DATABASE_URL` | Your PostgreSQL connection string |
| `USE_POSTGRES` | `true` |
| `FLASK_ENV` | `production` |

### For Azure PostgreSQL:
```
DATABASE_URL=postgresql://username:password@your-server.postgres.database.azure.com:5432/spotwego?sslmode=require
```

Or use individual variables:
- `DB_HOST`: your-server.postgres.database.azure.com
- `DB_NAME`: spotwego  
- `DB_USER`: your_username
- `DB_PASSWORD`: your_password
- `DB_PORT`: 5432

## Project Structure

```
spotwego_vercel/
├── api/
│   └── index.py          # Main Flask app
├── templates/
│   ├── dashboard.html    # Classic V3 dashboard
│   └── dashboard_v2.html # New V4.3 dashboard
├── static/
├── vercel.json           # Vercel configuration
├── requirements.txt      # Python dependencies
└── .env.example          # Environment variables template
```

## Features in V4.3

- ✅ Email test fix (recipient field)
- ✅ Weights tab for ranking configuration
- ✅ Sentiment score in rankings table
- ✅ 7 navigation sections
- ✅ Large city detection for better search
- ✅ All V3 features preserved

## Troubleshooting

### Cold Starts
Vercel serverless functions may have cold starts. First request might be slow.

### Database Connection
Make sure your PostgreSQL allows connections from Vercel IPs (or allow all with SSL).

### Template Not Found
If templates not found, check that templates folder is in project root, not in api/.
