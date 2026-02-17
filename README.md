# Market Sizing Tool

A Flask web app for TAM (Total Addressable Market) sizing using Prospeo's API.

## Features

- **Company Search Builder**: Define ICP with filters (location, B2B, headcount, industry, etc.)
- **Person Search Definitions**: Create multiple person queries (SDRs, sales reps, etc.)
- **Preview Mode**: Test queries with 1-2 credits before full runs
- **Adaptive Query Segmentation**: Automatically splits queries that exceed 25k result limit
- **Credit Estimation**: Shows estimated credits before running
- **Background Processing**: Long-running jobs with progress tracking
- **CSV Export**: Download results for further analysis
- **PostgreSQL Storage**: All results stored in queryable database

## Local Development

1. Create PostgreSQL database:
```bash
createdb market_sizing
```

2. Set environment variables:
```bash
cp .env.example .env
# Edit .env with your PROSPEO_API_KEY
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

4. Run the app:
```bash
python app.py
```

5. Open http://localhost:5000

## Deployment (Render)

1. Push to GitHub
2. Create new Web Service on Render
3. Connect your repo
4. Set `PROSPEO_API_KEY` environment variable
5. Render will automatically create PostgreSQL database

## Database Access (DBeaver)

After deploying to Render:
1. Go to your Render PostgreSQL dashboard
2. Copy the External Database URL
3. In DBeaver: New Connection → PostgreSQL → paste connection string

## Credit Model

- Company search: 1 credit per page (25 results)
- Person search: 1 credit per query (uses total_count, no pagination)
- Estimated credits shown before each run

## API Endpoints

- `GET /` - Filter builder UI
- `POST /api/preview` - Preview search (1-2 credits)
- `POST /api/jobs` - Start full job
- `GET /api/jobs/<id>` - Get job status
- `GET /api/jobs/<id>/results` - Get paginated results
- `GET /api/jobs/<id>/export` - Download CSV
- `POST /api/jobs/<id>/stop` - Stop running job
