# match_host_work_manager.py
"""
Match Host Work Manager - creates and processes match work orders
"""

# ----------------------------------------------
# Imports
# ————————————————————————
import json
import time
from typing import Dict, List

from sqlalchemy import text

# ----------------------------------------------
# Configuration
# ————————————————————————
from configurations.setting_coordination import MAX_NEW_JOBS, MIN_REMAINING_JOBS
from coordination.coordinator import create_work_tracker
from coordination.github_bridge import GitHubWorkBridge
from database.repositories.league_table_repository import LeagueTableRepository
from database.repositories.match_repository import MatchRepository
from database.repositories.matchday_repository import MatchdayRepository
from database.repositories.top_scorer_repository import TopScorerRepository


# ----------------------------------------------
# Class Definition
# ————————————————————————
class MatchHostWorkManager:
    """
    Manages match work distribution and result processing.
    Creates new work orders in batches and processes completed results.
    """

    def __init__(self, environment: str = "production", repo_url: str = None):
        # ***> Initialize environment and connectors <***
        self.environment = environment
        self.github_bridge = GitHubWorkBridge(repo_url=repo_url)
        self.progress_monitor = create_work_tracker(environment)

        # ***> Initialize repositories <***
        dbm = self.progress_monitor.db_service.db_manager
        self.match_repo = MatchRepository(dbm)
        self.matchday_repo = MatchdayRepository(dbm)
        self.league_repo = LeagueTableRepository(dbm)
        self.scorer_repo = TopScorerRepository(dbm)

        print("🏠 Match host work manager initialized for %s" % self.environment)

    def create_match_work_orders(self, limit: int = None) -> int:
        """
        Create up to `limit` work orders for competitions needing match data.
        """
        print("📋 Creating match work orders...")
        self.github_bridge._git_pull()

        # ***> Collect existing work keys <***
        existing = set()
        for folder in ["available", "claimed", "completed", "failed"]:
            path = self.github_bridge.folders[folder]
            for wf in path.glob("match_*.json"):
                try:
                    data = json.loads(wf.read_text())
                    key = "%s_%s" % (
                        data.get("competition_id"),
                        data.get("season_year"),
                    )
                    existing.add(key)
                except Exception:
                    continue

        created = 0
        competitions = self._get_competitions_needing_match_data()

        for comp in competitions:
            if limit is not None and created >= limit:
                break

            key = "%s_%s" % (comp["competition_id"], comp["season_year"])
            if key in existing:
                continue

            try:
                wid = self._create_single_match_work_order(comp)
                created += 1
                existing.add(key)
                print("📋 Created match work: %s" % wid)
            except Exception as e:
                print("❌ Failed creating work for %s: %s" % (key, e))

        print("✅ Created %d match work orders" % created)
        return created

    def _get_competitions_needing_match_data(self) -> List[Dict]:
        """
        Fetch competitions with teams but lacking match data.
        """
        try:
            with self.progress_monitor.db_service.db_manager.get_session() as session:
                qry = text(
                    """
                    SELECT DISTINCT c.competition_code,
                       c.competition_id,
                       c.competition_name,
                       t.season_year,
                       CAST(t.season_year AS INTEGER) season_int
                    FROM competitions c
                    JOIN teams t
                      ON c.competition_id = t.competition_id
                    WHERE t.season_year IS NOT NULL
                      AND CAST(t.season_year AS INTEGER) >= 2000
                      AND CAST(t.season_year AS INTEGER) <= 2024
                    ORDER BY season_int DESC
                    """
                )
                rows = session.execute(qry).fetchall()
                return [
                    {
                        "competition_code": r[0],
                        "competition_id": r[1],
                        "competition_name": r[2],
                        "season_year": r[3],
                    }
                    for r in rows
                ]
        except Exception as e:
            print("⚠️ Error getting competitions: %s" % e)
            return []

    def _create_single_match_work_order(self, competition: Dict) -> str:
        """
        Build and persist one match work order JSON file and push to Git.
        """
        wid = "match_%s_%s_%d" % (
            competition["competition_id"],
            competition["season_year"],
            int(time.time()),
        )

        incomplete = self._get_incomplete_matchdays(
            competition["competition_id"], competition["season_year"]
        )
        next_md = self._get_next_matchday_number(
            competition["competition_name"], competition["season_year"]
        )

        work = {
            "work_id": wid,
            "work_type": "match_data",
            "competition_id": competition["competition_id"],
            "competition_code": competition["competition_code"],
            "competition_name": competition["competition_name"],
            "season_year": competition["season_year"],
            "incomplete_matchdays": incomplete,
            "next_matchday_to_discover": next_md,
            "created_at": time.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "status": "available",
        }

        # ***> Write and push work file <***
        wf = self.github_bridge.folders["available"] / f"{wid}.json"
        with open(wf, "w") as f:
            json.dump(work, f, indent=2)
        self.github_bridge._git_add_commit_push("Add match work: %s" % wid)
        return wid

    def _get_incomplete_matchdays(self, comp_id: str, season: str) -> List[Dict]:
        """
        Identify matchdays where scraped count < expected total.
        """
        try:
            with self.progress_monitor.db_service.db_manager.get_session() as session:
                qry = text(
                    """
                    SELECT m.matchday_id,
                           m.number,
                           m.total_matches,
                           m.source_url,
                           COUNT(ma.match_id) actual
                    FROM matchdays m
                    LEFT JOIN matches ma
                      ON m.matchday_id = ma.matchday_id
                    WHERE m.competition = :c
                      AND m.season = :s
                    GROUP BY m.matchday_id, m.number,
                             m.total_matches, m.source_url
                    HAVING m.total_matches != COUNT(ma.match_id)
                    ORDER BY m.number
                    """
                )
                rows = session.execute(qry, {"c": comp_id, "s": season}).fetchall()
                return [
                    {
                        "matchday_id": r[0],
                        "number": r[1],
                        "total_matches": r[2],
                        "source_url": r[3],
                        "missing_matches": r[2] - r[4],
                    }
                    for r in rows
                ]
        except Exception:
            return []

    def _get_next_matchday_number(self, comp_name: str, season: str) -> int:
        """
        Determine the next sequential matchday number.
        """
        try:
            with self.progress_monitor.db_service.db_manager.get_session() as session:
                qry = text(
                    """
                    SELECT MAX(number)
                    FROM matchdays
                    WHERE competition = :c
                      AND season = :s
                    """
                )
                res = session.execute(qry, {"c": comp_name, "s": season}).fetchone()
                return (res[0] + 1) if res and res[0] else 1
        except Exception:
            return 1

    def get_match_work_status(self) -> Dict:
        """
        Return counts of work files in each GitHub folder.
        """
        self.github_bridge._git_pull()
        status = {}
        for name, folder in self.github_bridge.folders.items():
            if name == "claims":
                continue
            status[name] = len(list(folder.glob("match_*.json")))
        return status

    def run_match_host_cycle(self, max_cycles: int = 100):
        """
        Loop to create/process jobs, respecting thresholds.
        """
        print("🚀 Starting cycles up to %d" % max_cycles)
        total_new, total_proc = 0, 0

        for cycle in range(max_cycles):
            try:
                print("\n🔄 Cycle %d/%d" % (cycle + 1, max_cycles))

                stat = self.get_match_work_status()
                avail = stat.get("available", 0)

                # ***> Throttled creation <***
                if avail < MIN_REMAINING_JOBS:
                    new = self.create_match_work_orders(limit=MAX_NEW_JOBS)
                    total_new += new
                else:
                    print(
                        "ℹ️ Skipping creation; %d avail >= %d"
                        % (avail, MIN_REMAINING_JOBS)
                    )
                    new = 0

                proc = self.process_completed_match_work()
                total_proc += proc

                # ***> Status logging <***
                if new or proc or cycle % 10 == 0:
                    s = self.get_match_work_status()
                    print(
                        "📊 Status: %d avail | %d claimed | %d done"
                        % (s["available"], s["claimed"], s["completed"])
                    )

                # ***> Check for full completion <***
                if new == 0 and proc == 0:
                    if stat.get("claimed", 0) == 0 and avail == 0:
                        print("🎉 All work completed!")
                        break
                    time.sleep(120)

                time.sleep(30 if (new or proc) else 60)

            except Exception as e:
                print("💥 Cycle error: %s" % e)
                time.sleep(30)

        print("\n🏁 Finished: %d created, %d processed" % (total_new, total_proc))

    def process_completed_match_work(self) -> int:
        """
        Process items marked completed and archive them.
        """
        items = self.github_bridge.get_completed_work()
        match_items = [i for i in items if i.get("work_type") == "match_data"]
        count = 0
        for w in match_items:
            try:
                self._process_single_match_result(w)
                self.github_bridge.archive_processed_work(w)
                count += 1
            except Exception as e:
                wid = w.get("work_id", "unknown")
                print("❌ Error processing %s: %s" % (wid, e))
        if count:
            print("✅ Processed %d match work items" % count)
        return count

    def _process_single_match_result(self, work: Dict):
        """
        Log summary statistics from a completed work result.
        """
        wid = work["work_id"]
        print("📊 Processing match result: %s" % wid)
        summ = work.get("results", {}).get("summary_statistics", {})
        print(
            "✅ Completed %s: %d matches, %d matchdays"
            % (
                wid,
                summ.get("total_matches_scraped", 0),
                summ.get("total_matchdays_processed", 0),
            )
        )

    def cleanup(self):
        """
        Cleanup database and other resources.
        """
        if hasattr(self, "progress_monitor"):
            self.progress_monitor.db_service.cleanup()


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Match Host Work Manager")
    parser.add_argument("--repo-url", help="GitHub repository URL")
    parser.add_argument("--environment", default="production")
    parser.add_argument("--max-cycles", type=int, default=500)
    args = parser.parse_args()
    mgr = MatchHostWorkManager(environment=args.environment, repo_url=args.repo_url)
    try:
        mgr.run_match_host_cycle(args.max_cycles)
    finally:
        mgr.cleanup()


if __name__ == "__main__":
    main()
