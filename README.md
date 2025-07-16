# Scraping Work Distribution

This repository manages distributed scraping work orders.

## Folder Structure

- `available_competitions/` - Work orders ready to be claimed by workers
- `claimed_competitions/` - Work orders currently being processed
- `completed_competitions/` - Finished work with results
- `failed_competitions/` - Failed work orders for retry

## Usage

### Host Machine (Database + Task Creator)
```bash
python host_work_manager.py --repo-url <github-repo-url>
```

### Worker Machines (Task Processors)
```bash
python worker_main.py --repo-url <github-repo-url>
```

## Work Order Format

```json
{
  "work_id": "comp_GB1_uuid",
  "competition_id": "GB1",
  "competition_url": "https://...",
  "completed_seasons": ["2020", "2021"],
  "created_at": "2025-07-16T10:30:00Z"
}
```
