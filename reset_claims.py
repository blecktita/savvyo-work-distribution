# Create a simple utility script to reset all claims
# Save this as reset_claims.py

from coordination import create_work_tracker
from sqlalchemy import text

def reset_all_competition_claims(environment="production"):
    """
    Reset all competition claims so they can be claimed fresh.
    """
    print("🔄 Resetting all competition claims...")
    
    tracker = create_work_tracker(environment)
    
    try:
        with tracker.db_service.transaction() as session:
            # Reset all competitions to pending status
            result = session.execute(text("""
                UPDATE competition_progress 
                SET status = 'pending', 
                    worker_id = NULL, 
                    started_at = NULL,
                    completed_at = NULL,
                    error_message = NULL
                WHERE status != 'completed'
                RETURNING competition_id
            """))
            
            reset_competitions = [row[0] for row in result.fetchall()]
            
            # Reset all seasons to pending status
            season_result = session.execute(text("""
                UPDATE season_progress 
                SET status = 'pending', 
                    worker_id = NULL, 
                    started_at = NULL,
                    completed_at = NULL,
                    error_message = NULL,
                    retry_count = 0
                WHERE status != 'completed'
                RETURNING competition_id, season_id
            """))
            
            reset_seasons = [f"{row[0]}-{row[1]}" for row in season_result.fetchall()]
            
            session.commit()
            
        print(f"✅ Reset {len(reset_competitions)} competitions:")
        for comp_id in reset_competitions[:10]:  # Show first 10
            print(f"   • {comp_id}")
        if len(reset_competitions) > 10:
            print(f"   • ... and {len(reset_competitions) - 10} more")
            
        print(f"✅ Reset {len(reset_seasons)} seasons")
        print("🎯 All claims released - ready for fresh start!")
        
    except Exception as e:
        print(f"❌ Error resetting claims: {str(e)}")
    finally:
        tracker.db_service.cleanup()

def show_current_claims(environment="production"):
    """
    Show what's currently claimed by which workers.
    """
    print("📊 Current competition claims:")
    
    tracker = create_work_tracker(environment)
    
    try:
        with tracker.db_service.transaction() as session:
            result = session.execute(text("""
                SELECT competition_id, status, worker_id, started_at
                FROM competition_progress 
                WHERE worker_id IS NOT NULL
                ORDER BY started_at DESC
            """))
            
            claims = result.fetchall()
            
            if not claims:
                print("   No active claims found")
            else:
                for comp_id, status, worker_id, started_at in claims:
                    worker_short = worker_id[-8:] if worker_id else "unknown"
                    time_str = started_at.strftime("%H:%M:%S") if started_at else "unknown"
                    print(f"   • {comp_id}: {status} by {worker_short} at {time_str}")
                    
    except Exception as e:
        print(f"❌ Error showing claims: {str(e)}")
    finally:
        tracker.db_service.cleanup()

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == "show":
        show_current_claims()
    else:
        print("This will reset ALL competition and season claims.")
        print("Other workers will lose their current work!")
        confirm = input("Type 'RESET' to confirm: ")
        
        if confirm == 'RESET':
            reset_all_competition_claims()
        else:
            print("❌ Reset cancelled")
            show_current_claims()