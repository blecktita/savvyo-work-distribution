#!/usr/bin/env python3
"""
All-in-One Scraper: Gets matchday + all match details in one command
No database required - saves everything to JSON files
"""

import json
import time
import argparse
from pathlib import Path

def scrape_complete_matchday(matchday_url, season="2023-24", matchday=1, output_dir="complete_data"):
    """
    Single command to get EVERYTHING:
    1. Matchday data (league table, top scorers, match list)
    2. Detailed data for ALL matches found
    """
    
    # Import your extractors
    try:
        from test_player import MatchdayExtractor, CleanMatchExtractor
    except ImportError:
        print("❌ Error: Cannot import extractors from test_player.py")
        print("Make sure test_player.py is in the same directory")
        return
    
    # Create output directories
    Path(output_dir).mkdir(exist_ok=True)
    
    print(f"🏈 COMPLETE MATCHDAY EXTRACTION")
    print(f"📅 Season: {season}, Matchday: {matchday}")
    print(f"🔗 URL: {matchday_url}")
    print("=" * 60)
    
    # Initialize extractors
    matchday_extractor = MatchdayExtractor()
    match_extractor = CleanMatchExtractor()
    
    # STEP 1: Extract matchday data
    print("📊 Step 1: Extracting matchday overview...")
    try:
        matchday_data = matchday_extractor.extract_from_transfermarkt_url(matchday_url, matchday, season)
        print(f"✅ Found {len(matchday_data.matches)} matches")
        
        # Save matchday data
        matchday_file = f"{output_dir}/{season}_matchday_{matchday:02d}_complete.json"
        
    except Exception as e:
        print(f"❌ Failed to extract matchday data: {str(e)}")
        return
    
    # STEP 2: Extract all match details
    print(f"\n⚽ Step 2: Extracting details for all {len(matchday_data.matches)} matches...")
    
    detailed_matches = []
    success_count = 0
    
    for i, match in enumerate(matchday_data.matches, 1):
        if not match.match_report_url:
            print(f"  ⚠️  Match {i}: No report URL - skipping")
            continue
        
        try:
            home_name = match.home_team.get('name', 'Unknown') if match.home_team else 'Unknown'
            away_name = match.away_team.get('name', 'Unknown') if match.away_team else 'Unknown'
            score = match.final_score.get('display', 'N/A') if match.final_score else 'N/A'
            
            print(f"  ⚽ Match {i}/{len(matchday_data.matches)}: {home_name} vs {away_name} ({score})")
            
            # Build full URL
            match_url = match.match_report_url
            if not match_url.startswith('http'):
                match_url = f"https://www.transfermarkt.com{match_url}"
            
            # Extract detailed match data
            match_detail = match_extractor.extract_from_url(match_url)
            
            # Convert to dict and add to list
            detailed_matches.append({
                'match_overview': match.__dict__,  # Basic info from matchday
                'match_details': match_detail.__dict__  # Detailed info from match page
            })
            
            success_count += 1
            print(f"    ✅ Extracted: {len(match_detail.goals)} goals, {len(match_detail.cards)} cards, {len(match_detail.substitutions)} subs")
            
            # Be respectful to the server
            time.sleep(2)
            
        except Exception as e:
            print(f"    ❌ Failed: {str(e)}")
            continue
    
    # STEP 3: Save everything in one comprehensive file
    print(f"\n💾 Step 3: Saving complete data...")
    
    complete_data = {
        'matchday_info': matchday_data.matchday_info,
        'league_table': matchday_data.league_table,
        'top_scorers': matchday_data.top_scorers,
        'matchday_summary': matchday_data.matchday_summary,
        'extraction_metadata': {
            'extraction_time': matchday_data.metadata['extraction_time'],
            'total_matches_found': len(matchday_data.matches),
            'total_matches_detailed': success_count,
            'source_url': matchday_url
        },
        'matches_with_details': detailed_matches  # Complete data for each match
    }
    
    # Save the comprehensive file
    with open(matchday_file, 'w', encoding='utf-8') as f:
        json.dump(complete_data, f, indent=2, ensure_ascii=False, default=str)
    
    # Summary
    print(f"\n🎉 EXTRACTION COMPLETE!")
    print(f"📁 Complete file: {matchday_file}")
    print(f"📊 Matchday data: ✅ (league table, top scorers, etc.)")
    print(f"⚽ Match details: {success_count}/{len(matchday_data.matches)} matches extracted")
    print(f"📂 Output directory: {output_dir}")
    
    return {
        'file_path': matchday_file,
        'matches_extracted': success_count,
        'total_matches': len(matchday_data.matches)
    }

def main():
    parser = argparse.ArgumentParser(description='Complete Matchday + Matches Scraper')
    parser.add_argument('--season', default='2023-24', help='Season (e.g., 2023-24)')
    parser.add_argument('--matchday', type=int, default=1, help='Matchday number')
    parser.add_argument('--url', help='Custom matchday URL')
    parser.add_argument('--output', default='complete_data', help='Output directory')
    
    args = parser.parse_args()
    
    # Build URL if not provided
    if args.url:
        matchday_url = args.url
    else:
        season_year = args.season.split('-')[0]  # "2023-24" -> "2023"
        matchday_url = f"https://www.transfermarkt.com/premier-league/spieltag/wettbewerb/GB1/saison_id/{season_year}/spieltag/{args.matchday}"
    
    try:
        result = scrape_complete_matchday(matchday_url, args.season, args.matchday, args.output)
        if result:
            print(f"\n📈 SUCCESS: {result['matches_extracted']}/{result['total_matches']} matches extracted")
        
    except Exception as e:
        print(f"❌ Error: {str(e)}")

if __name__ == "__main__":
    main()