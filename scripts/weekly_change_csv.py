# %%
# imports and initialization
import csv
import glob
import json
import logging
import os
from datetime import datetime, timedelta
from pathlib import Path

# number of days back to compare the most recent data to
comparison_age_days = 7
second_comparison_age_days = 30  # Additional comparison period

# Get cache directory from environment variable
STATS_DIR = Path(os.getenv('DATA_CACHE_DIR', 'data')) / 'daily_statistics'
if not STATS_DIR.exists():
    raise ValueError(f"Statistics directory {STATS_DIR} does not exist")

# Create weekly_changes directory if it doesn't exist
WEEKLY_CHANGES_DIR = Path(os.getenv('DATA_CACHE_DIR', 'data')) / 'weekly_changes'
WEEKLY_CHANGES_DIR.mkdir(parents=True, exist_ok=True)

def get_date_from_filename(filename: Path):
    """Extract date from filename like '20250310T070248.json'"""
    base = os.path.basename(filename)
    date_str = base.replace('.json', '')
    return datetime.strptime(date_str, '%Y%m%dT%H%M%S')

# Get all JSON files and sort them by date
json_files = glob.glob(os.path.join(STATS_DIR, '*.json'))
json_files.sort(key=get_date_from_filename)

if not json_files:
    logging.error(f"No JSON files found in {STATS_DIR}")
    exit(1)

# %%
# Initialize tracking dictionaries
org_stats = {}  # {org_id: {title, all_time_adds, all_time_removes, etc}}

# Get current date from most recent file
current_date = get_date_from_filename(json_files[-1])
compare_date = current_date - timedelta(days=comparison_age_days)
second_compare_date = current_date - timedelta(days=second_comparison_age_days)

# %%
# Process each file chronologically
for json_file in json_files:
    file_date = get_date_from_filename(json_file)
    # For 7-day period, only count files from the last 7 days
    is_first_period = file_date >= compare_date
    # For 30-day period, count all files from the last 30 days
    is_second_period = file_date >= second_compare_date
    
    with open(json_file, 'r') as f:
        data = json.load(f)
        
    # Process additions
    for added in data.get('deltas', {}).get('added', []):
        org_id = added['organization']['id']
        if org_id not in org_stats:
            org_stats[org_id] = {
                'title': added['organization']['title'],
                'all_time_removes': 0,
                'all_time_adds': 0,
                'first_period_removes': 0,
                'first_period_adds': 0,
                'second_period_removes': 0,
                'second_period_adds': 0,
                'current_total': 0
            }
        org_stats[org_id]['all_time_adds'] += 1
        # Only count in first period if it's within the last 7 days
        if is_first_period:
            org_stats[org_id]['first_period_adds'] += 1
        # Count in second period if it's within the last 30 days
        if is_second_period:
            org_stats[org_id]['second_period_adds'] += 1
            
    # Process removals
    for removed in data.get('deltas', {}).get('removed', []):
        org_id = removed['organization']['id']
        if org_id not in org_stats:
            org_stats[org_id] = {
                'title': removed['organization']['title'],
                'all_time_removes': 0,
                'all_time_adds': 0,
                'first_period_removes': 0,
                'first_period_adds': 0,
                'second_period_removes': 0,
                'second_period_adds': 0,
                'current_total': 0
            }
        org_stats[org_id]['all_time_removes'] += 1
        if is_first_period:
            org_stats[org_id]['first_period_removes'] += 1
        if is_second_period:
            org_stats[org_id]['second_period_removes'] += 1

    # Update current totals from most recent file
    if json_file == json_files[-1]:
        for org in data['counts']['organizations']:
            org_id = org['id']
            if org_id in org_stats:
                org_stats[org_id]['current_total'] = org['catalog_count']

# %%
# Write results to CSV
output_timestamp = f"{current_date.strftime('%Y%m%dT%H%M%S')}"
output_file = WEEKLY_CHANGES_DIR / f'organization_changes_{output_timestamp}.csv'

# Custom sorting function
def sort_key(item):
    org_id, stats = item
    net_change = stats['second_period_adds'] - stats['second_period_removes']
    # First sort by category (negative=0, positive=1, zero=2)
    if net_change < 0:
        category = 0
    elif net_change > 0:
        category = 1
    else:
        category = 2
    # Then sort by absolute net change within category (descending)
    # Then by title alphabetically
    return (category, -abs(net_change), stats['title'])

# Convert org_stats to a list and sort using custom sort key
sorted_stats = sorted(org_stats.items(), key=sort_key)

with open(output_file, 'w', newline='') as f:
    writer = csv.writer(f)
    writer.writerow([
        'Organization Title',
        'All-time Removals',
        'All-time Removals %',
        'All-time Additions',
        'All-time Additions %',
        'All-time Net Change',
        'All-time Net Change %',
        f'{second_comparison_age_days}-day Removals',
        f'{second_comparison_age_days}-day Removals %',
        f'{second_comparison_age_days}-day Additions',
        f'{second_comparison_age_days}-day Additions %',
        f'{second_comparison_age_days}-day Net Change',
        f'{second_comparison_age_days}-day Net Change %',
        f'{comparison_age_days}-day Removals',
        f'{comparison_age_days}-day Removals %',
        f'{comparison_age_days}-day Additions',
        f'{comparison_age_days}-day Additions %',
        f'{comparison_age_days}-day Net Change',
        f'{comparison_age_days}-day Net Change %',
        'Current Total'
    ])
    
    for org_id, stats in sorted_stats:
        current_total = stats['current_total']
        # Calculate percentages, handling division by zero
        def safe_percentage(numerator, denominator, force_negative=False):
            if denominator == 0:
                return '0.0%'
            percentage = (numerator / denominator) * 100
            if force_negative:
                return f"-{abs(percentage):.1f}%"
            sign = '+' if percentage > 0 else '-' if percentage < 0 else ''
            return f"{sign}{abs(percentage):.1f}%"
            
        writer.writerow([
            stats['title'],
            stats['all_time_removes'],
            safe_percentage(stats['all_time_removes'], current_total, force_negative=True),
            stats['all_time_adds'],
            safe_percentage(stats['all_time_adds'], current_total),
            stats['all_time_adds'] - stats['all_time_removes'],
            safe_percentage(stats['all_time_adds'] - stats['all_time_removes'], current_total),
            stats['first_period_removes'],
            safe_percentage(stats['first_period_removes'], current_total, force_negative=True),
            stats['first_period_adds'],
            safe_percentage(stats['first_period_adds'], current_total),
            stats['first_period_adds'] - stats['first_period_removes'],
            safe_percentage(stats['first_period_adds'] - stats['first_period_removes'], current_total),
            stats['second_period_removes'],
            safe_percentage(stats['second_period_removes'], current_total, force_negative=True),
            stats['second_period_adds'],
            safe_percentage(stats['second_period_adds'], current_total),
            stats['second_period_adds'] - stats['second_period_removes'],
            safe_percentage(stats['second_period_adds'] - stats['second_period_removes'], current_total),
            current_total
        ])

logging.info(f"Statistics written to {output_file}")
