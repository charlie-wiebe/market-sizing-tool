#!/bin/bash
# Setup cron job for daily HubSpot cache sync
# Run this script once to set up automatic cache refresh

# Add to crontab - runs daily at 2 AM
(crontab -l 2>/dev/null; echo "0 2 * * * cd /opt/render/project/src && python sync_hubspot_cache.py >> /opt/render/project/src/logs/hubspot_sync.log 2>&1") | crontab -

echo "✅ Cron job created for daily HubSpot cache sync at 2 AM"
echo "Logs will be written to: /opt/render/project/src/logs/hubspot_sync.log"
echo ""
echo "To view cron jobs: crontab -l"
echo "To remove cron job: crontab -r"
echo "To run manual sync: python sync_hubspot_cache.py"
echo "To run full sync: python sync_hubspot_cache.py --full"
