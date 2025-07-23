#!/usr/bin/env python3
"""
MOMENT OF TRUTH - Simple test script to verify the entire pipeline works
Tests: Extraction → Conversion → Database Storage
"""

import sys

# Import your existing classes (adjust imports as needed)
from extractors.extractor_matchday import MatchdayExtractor, CleanMatchExtractor
from database.orchestrators.match_orchestrator import MatchDataOrchestrator

def test_single_match_pipeline():
    """Test the complete pipeline with a single match"""
    
    print("🧪 MOMENT OF TRUTH - Testing Complete Pipeline")
    print("=" * 60)
    
    # Test URLs
    matchday_url = "https://www.transfermarkt.com/premier-league/spieltag/wettbewerb/GB1/saison_id/2023/spieltag/1"
    
    try:
        # Step 1: Extract matchday data
        print("1️⃣ Extracting matchday data...")
        matchday_extractor = MatchdayExtractor()
        matchday_data = matchday_extractor.extract_from_transfermarkt_url(
            matchday_url, matchday=1, season="2023-24"
        )
        
        print(f"   ✅ Found {len(matchday_data.matches)} matches")
        
        if not matchday_data.matches:
            print("   ❌ No matches found - stopping test")
            return False
        
        # Pick the first match for detailed testing
        first_match = matchday_data.matches[0]
        print(f"   📋 Testing with: {first_match.home_team.get('name', 'Unknown')} vs {first_match.away_team.get('name', 'Unknown')}")
        print(f"   🔗 Match report URL: {first_match.match_report_url}")
        
        # Step 2: Extract detailed match data
        print("\n2️⃣ Extracting detailed match data...")
        match_extractor = CleanMatchExtractor()
        
        # Build full URL for detailed extraction
        detail_url = first_match.match_report_url
        if not detail_url.startswith('http'):
            detail_url = f"https://www.transfermarkt.com{detail_url}"
        
        print(f"   🎯 Scraping: {detail_url}")
        match_detail = match_extractor.extract_from_url(detail_url)
        
        print(f"   ✅ Match: {match_detail.home_team.name} vs {match_detail.away_team.name}")
        print(f"   ⚽ Score: {match_detail.score.home_final}-{match_detail.score.away_final}")
        print(f"   👥 Home lineup: {len(match_detail.home_lineup)} players")
        print(f"   👥 Away lineup: {len(match_detail.away_lineup)} players")
        print(f"   🥅 Goals: {len(match_detail.goals)}")
        print(f"   🟨 Cards: {len(match_detail.cards)}")
        print(f"   🔄 Substitutions: {len(match_detail.substitutions)}")
        
        # Step 3: Check what URLs we have
        print("\n3️⃣ Checking URL data...")
        print(f"   📍 Original match_report_url: {first_match.match_report_url}")
        
        extraction_metadata = match_detail.extraction_metadata or {}
        source_url = extraction_metadata.get('source_url')
        print(f"   📍 Extraction source_url: {source_url}")
        
        # Step 4: Test database save
        print("\n4️⃣ Testing database save...")
        try:
            orchestrator = MatchDataOrchestrator(environment="production")
            
            if not orchestrator.is_available:
                print("   ❌ Database orchestrator not available")
                return False
            
            # Save matchday info
            print("   💾 Saving matchday data...")
            matchday_success = orchestrator.save_matchday_container(matchday_data)
            if matchday_success:
                print("   ✅ Matchday data saved successfully")
            else:
                print("   ⚠️ Matchday data save failed")
            
            # Save detailed match
            print("   💾 Saving detailed match data...")
            match_success = orchestrator.save_match_detail(match_detail)
            if match_success:
                print("   ✅ Match detail saved successfully")
            else:
                print("   ❌ Match detail save failed")
                return False
            
        except Exception as e:
            print(f"   ❌ Database error: {e}")
            return False
        
        # Step 5: Verify database content
        print("\n5️⃣ Verifying database content...")
        try:
            saved_match = orchestrator.get_match_by_id(match_detail.match_info.match_id)
            
            if saved_match:
                print("   ✅ Match found in database!")
                print(f"   📋 Match ID: {saved_match.get('match_id')}")
                print(f"   🏠 Home team: {saved_match.get('home_team_name')}")
                print(f"   🏃 Away team: {saved_match.get('away_team_name')}")
                print(f"   ⚽ Score: {saved_match.get('home_final_score')}-{saved_match.get('away_final_score')}")
                print(f"   🔗 Match report URL: {saved_match.get('match_report_url')}")
                print(f"   📍 Source URL: {saved_match.get('source_url')}")
                print(f"   📅 Date: {saved_match.get('date')}")
                print(f"   🏟️ Venue: {saved_match.get('venue')}")
                
                # Check if URLs are properly stored
                if saved_match.get('match_report_url') and saved_match.get('source_url'):
                    print("   ✅ Both URLs properly stored!")
                else:
                    print("   ⚠️ URL fields missing or empty")
                    print(f"      match_report_url: {saved_match.get('match_report_url')}")
                    print(f"      source_url: {saved_match.get('source_url')}")
                
            else:
                print("   ❌ Match not found in database")
                return False
                
        except Exception as e:
            print(f"   ❌ Database verification error: {e}")
            return False
        
        print("\n🎉 SUCCESS! Complete pipeline works!")
        return True
        
    except Exception as e:
        print(f"\n❌ PIPELINE FAILED: {e}")
        import traceback
        print(traceback.format_exc())
        return False

def test_url_mapping():
    """Test just the URL mapping logic"""
    print("\n🔍 Testing URL Mapping Logic")
    print("-" * 40)
    
    # Test data
    original_relative = "/spielbericht/index/spielbericht/4087933"
    converted_absolute = f"https://www.transfermarkt.com{original_relative}"
    
    print(f"Original relative URL: {original_relative}")
    print(f"Converted absolute URL: {converted_absolute}")
    
    # This is what should go in database:
    print(f"\nDatabase should store:")
    print(f"  match_report_url: {original_relative}")  # Keep original
    print(f"  source_url: {converted_absolute}")       # Store what we actually scraped

def main():
    """Run the moment of truth test"""
    
    print("🚀 Starting Moment of Truth Test")
    print("This will test the complete extraction → database pipeline")
    print()
    
    # Test URL mapping first
    test_url_mapping()
    
    # Test the complete pipeline
    success = test_single_match_pipeline()
    
    if success:
        print("\n🎯 MOMENT OF TRUTH: PASSED! ✅")
        print("The complete pipeline works as expected.")
    else:
        print("\n💥 MOMENT OF TRUTH: FAILED! ❌")
        print("Check the errors above and fix the issues.")
    
    return success

if __name__ == "__main__":
    try:
        success = main()
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        print("\n\n⏹️ Test interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n💥 Unexpected error: {e}")
        import traceback
        print(traceback.format_exc())
        sys.exit(1)