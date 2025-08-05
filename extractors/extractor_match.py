# extractors/extractor_match.py
import re
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import backoff
from bs4 import BeautifulSoup, Tag

from logger import (
    Card,
    Goal,
    MatchContextual,
    MatchdayContainer,
    MatchDetail,
    MatchInfo,
    MatchItems,
    Player,
    Score,
    Substitution,
    Team,
)


class MatchdayExtractor:
    """
    Extracts general contextual matchday data
    """

    def __init__(self):
        print("placeholder")

    def _get_match_metadata(self, table) -> Dict[str, Any]:
        metadata = {}

        # Extract date and time
        date_rows = table.find_all(MatchItems.TREE)
        for row in date_rows:
            cell = row.find(*MatchItems.TREE_DATE_SPAN)
            if not cell:
                continue

            text = cell.get_text(strip=True)

            # Date and time extraction
            date_link = cell.find(MatchItems.LINK)
            if date_link and MatchItems.DATE_HREF_KEYWORD in date_link.get(
                MatchItems.HREF_ATTR, MatchItems.EMPTY_STRING
            ):
                date_text = date_link.text.strip()
                time_match = re.search(MatchItems.TIME_REGEX, text)

                # Parse date
                try:
                    if MatchItems.SCORE_SEPARATOR in date_text:  # Using comma separator
                        date_obj = datetime.strptime(
                            date_text, MatchItems.INPUT_DATE_FORMAT
                        )
                        metadata["date"] = date_obj.strftime(
                            MatchItems.OUTPUT_DATE_FORMAT
                        )
                except:
                    metadata["date"] = date_text

                # Parse time
                if time_match:
                    time_str = time_match.group(1)
                    if time_match.group(
                        2
                    ) == MatchItems.PM_INDICATOR and not time_str.startswith(
                        MatchItems.NOON_HOUR
                    ):
                        hour, minute = time_str.split(MatchItems.SCORE_SEPARATOR)
                        hour = str(int(hour) + MatchItems.HOURS_IN_HALF_DAY)
                        time_str = f"{hour}{MatchItems.SCORE_SEPARATOR}{minute}"
                    metadata["time"] = time_str

                # Extract day of week
                for day in MatchItems.DAYS_OF_WEEK:
                    if day in text or day[:3] in text:
                        metadata["day_of_week"] = day
                        break

            # Referee extraction
            if MatchItems.REFEREE_LABEL in text:
                referee_link = cell.find(MatchItems.LINK)
                if referee_link:
                    metadata["referee"] = {
                        "name": referee_link.text.strip(),
                        "profile_url": referee_link.get(
                            MatchItems.HREF_ATTR, MatchItems.EMPTY_STRING
                        ),
                    }

            # Attendance extraction
            if MatchItems.ATTENDANCE_ICON_CLASS in str(cell):
                attendance_match = re.search(MatchItems.ATTENDANCE_REGEX, text)
                if attendance_match:
                    attendance_str = (
                        attendance_match.group(1)
                        .replace(",", MatchItems.EMPTY_STRING)
                        .replace(".", MatchItems.EMPTY_STRING)
                    )
                    try:
                        metadata["attendance"] = int(attendance_str)
                    except:
                        pass

        return metadata

    def _get_team_data(self, team_cell) -> Dict[str, Any]:
        """Extract team information from HTML cells using MatchItems constants"""
        team_data: Dict[str, Any] = {}

        # Find team link
        team_link = team_cell.find(MatchItems.LINK)

        if team_link:
            team_data["name"] = team_link.get(
                MatchItems.TITLE_ATTR, MatchItems.EMPTY_STRING
            ).strip()
            team_data["profile_url"] = team_link.get(
                MatchItems.HREF_ATTR, MatchItems.EMPTY_STRING
            )
            team_data["short_name"] = team_link.text.strip()

        # Extract league position
        position_elem = team_cell.find(*MatchItems.POSITION_SPAN)
        if position_elem:
            pos_text = position_elem.text.strip()
            match = re.search(MatchItems.POSITION_REGEX, pos_text)
            if match:
                team_data["league_position"] = int(match.group(1))

        # Extract logo URL
        img_elem = team_cell.find(MatchItems.IMAGE)
        if img_elem:
            team_data["logo_url"] = img_elem.get(
                MatchItems.SRC_ATTR, MatchItems.EMPTY_STRING
            )
        return team_data

    def _get_match_id(self, match_report_url: str) -> Optional[str]:
        """Extract match ID from match report URL"""
        if not match_report_url:
            return None
        match = re.search(MatchItems.MATCH_ID_REGEX, match_report_url)
        return match.group(1) if match else None

    def _get_match_events(self, table) -> List[Dict[str, Any]]:
        """Extract match events using WORKING logic from snippet 3"""
        events = []

        # Find all event rows
        event_rows = table.find_all(MatchItems.TREE, class_=MatchItems.EVENT_ROW_CLASS)

        for row in event_rows:
            cells = row.find_all(MatchItems.TREE_DATE)
            if len(cells) < MatchItems.MIN_EVENT_CELLS:
                continue

            event = {}

            # Determine which team scored based on cell content
            left_team_cell = cells[MatchItems.EVENT_LEFT_TEAM_IDX]  # Home team events
            minute_cell = cells[
                MatchItems.EVENT_MINUTE_HOME_IDX
            ]  # Minute (if home team)
            score_cell = cells[MatchItems.EVENT_SCORE_IDX]  # Score
            away_minute_cell = cells[
                MatchItems.EVENT_MINUTE_AWAY_IDX
            ]  # Minute (if away team)
            right_team_cell = cells[MatchItems.EVENT_RIGHT_TEAM_IDX]  # Away team events

            # Check if it's a home team event
            if (
                minute_cell.text.strip()
                and minute_cell.text.strip() != MatchItems.EMPTY_STRING
            ):
                # Home team event
                event["team"] = MatchItems.HOME_TEAM
                minute_match = re.search(MatchItems.MINUTE_REGEX, minute_cell.text)
                if minute_match:
                    event["minute"] = int(minute_match.group())
                player_info = self._get_player_info(left_team_cell)
            elif (
                away_minute_cell.text.strip()
                and away_minute_cell.text.strip() != MatchItems.EMPTY_STRING
            ):
                # Away team event
                event["team"] = MatchItems.AWAY_TEAM
                minute_match = re.search(MatchItems.MINUTE_REGEX, away_minute_cell.text)
                if minute_match:
                    event["minute"] = int(minute_match.group())
                player_info = self._get_player_info(right_team_cell)
            else:
                continue

            # Extract score after event
            score_text = score_cell.text.strip()
            if MatchItems.SCORE_SEPARATOR in score_text:
                event["score_after"] = score_text

            # Determine event type
            if MatchItems.GOAL_ICON in str(row):
                event["type"] = MatchItems.GOAL_TYPE
            elif MatchItems.YELLOW_CARD_CLASS in str(row):
                event["type"] = MatchItems.YELLOW_CARD_TYPE
            elif MatchItems.RED_CARD_CLASS in str(row):
                event["type"] = MatchItems.RED_CARD_TYPE

            # Add player information
            if player_info:
                event["player"] = player_info

            events.append(event)

        return sorted(events, key=lambda x: x.get("minute", 0))

    def _get_player_info(self, cell) -> Optional[Dict[str, str]]:
        """Extract player information from event cell - WORKING logic from snippet 3"""
        player_link = cell.find(MatchItems.LINK)
        if not player_link:
            return None

        # Find both full name and short name spans
        full_name_span = cell.find(
            MatchItems.SPAN, class_=MatchItems.HIDE_FOR_SMALL_CLASS
        )
        short_name_span = cell.find(
            MatchItems.SPAN, class_=MatchItems.SHOW_FOR_SMALL_CLASS
        )

        player_info = {
            "profile_url": player_link.get(
                MatchItems.HREF_ATTR, MatchItems.EMPTY_STRING
            )
        }

        if full_name_span and full_name_span.find(MatchItems.LINK):
            player_info["name"] = full_name_span.find(MatchItems.LINK).text.strip()
        elif player_link:
            player_info["name"] = player_link.get(
                MatchItems.TITLE_ATTR, player_link.text.strip()
            )

        if short_name_span and short_name_span.find(MatchItems.LINK):
            player_info["short_name"] = short_name_span.find(
                MatchItems.LINK
            ).text.strip()
        else:
            # Generate short name from full name if not available
            full_name = player_info.get("name", MatchItems.EMPTY_STRING)
            if full_name:
                name_parts = full_name.split()
                if len(name_parts) >= 2:
                    player_info["short_name"] = f"{name_parts[0][0]}. {name_parts[-1]}"
                else:
                    player_info["short_name"] = full_name

        return player_info

    def _get_community_predictions(self, table) -> Dict[str, float]:
        """Extract community prediction percentages (all literals from MatchItems)."""
        preds = {
            MatchItems.HOME_WIN_PERCENTAGE: MatchItems.DEFAULT_PERCENTAGE,
            MatchItems.DRAW_PERCENTAGE: MatchItems.DEFAULT_PERCENTAGE,
            MatchItems.AWAY_WIN_PERCENTAGE: MatchItems.DEFAULT_PERCENTAGE,
        }

        # find the prediction row
        row = table.find(MatchItems.TREE, class_=MatchItems.PRED_ROW_CLASS)
        if not row:
            return preds

        # iterate all cells and spans
        cells = row.find_all(MatchItems.CELL)
        for cell in cells:
            for span in cell.find_all(MatchItems.SPAN):
                title = span.get(MatchItems.TITLE_ATTR, MatchItems.EMPTY_STRING)
                text = span.text.strip()

                match = re.search(MatchItems.PERCENT_REGEX, text)
                if not match:
                    continue
                pct = float(match.group(1))

                cls_list = span.get(MatchItems.CLASS_ATTR, [])
                if MatchItems.HOME_TITLE in title:
                    if MatchItems.HOME_BAR_CLASS in cls_list:
                        preds[MatchItems.HOME_WIN_PERCENTAGE] = pct
                    elif MatchItems.AWAY_BAR_CLASS in cls_list:
                        preds[MatchItems.AWAY_WIN_PERCENTAGE] = pct

                elif MatchItems.DRAW_TITLE in title:
                    preds[MatchItems.DRAW_PERCENTAGE] = pct

        return preds

    def _get_match(self, match_div) -> Optional[MatchContextual]:
        # 1) find the container table
        table = match_div.find(MatchItems.BRANCH)
        if not table:
            return None

        # 2) metadata
        metadata = self._get_match_metadata(table)

        # 3) main row
        main_row = table.find(MatchItems.TREE, class_=MatchItems.TABLE)
        if not main_row:
            return None

        cells = main_row.find_all(MatchItems.TREE_DATE)
        if len(cells) < MatchItems.MIN_CELL_COUNT:
            return None

        # 4) teams
        home_team = self._get_team_data(cells[MatchItems.HOME_IDX])
        away_team = self._get_team_data(cells[MatchItems.AWAY_IDX])

        # 5) score + report link
        final_score = {}
        report_url = MatchItems.EMPTY_STRING
        match_id = MatchItems.EMPTY_STRING

        score_link = cells[MatchItems.SCORE_IDX].find(MatchItems.LINK)
        if score_link:
            # parse score
            score_span = score_link.find(MatchItems.SPAN)
            if score_span:
                text = score_span.text.strip()
                final_score = {"display": text}
                if MatchItems.SCORE_SEPARATOR in text:
                    try:
                        h, a = map(int, text.split(MatchItems.SCORE_SEPARATOR))
                        final_score.update(home=h, away=a)
                    except ValueError:
                        pass

            # extract URL & ID
            report_url = score_link.get(MatchItems.HREF_ATTR, MatchItems.EMPTY_STRING)
            if report_url:
                if report_url.startswith("/"):
                    report_url = f"https://www.transfermarkt.com{report_url}"
                match_id = self._get_match_id(report_url)

        # 6) fallback footer link
        if not report_url:
            footer = match_div.find(MatchItems.DIV, class_=MatchItems.FOOTER_CLASS)
            if footer:
                link = footer.find(MatchItems.LINK, href=True)
                href = (
                    link.get(MatchItems.HREF_ATTR, MatchItems.EMPTY_STRING)
                    if link
                    else MatchItems.EMPTY_STRING
                )
                if MatchItems.REPORT_KEYWORD in href:
                    if href.startswith("/"):
                        href = f"https://www.transfermarkt.com{href}"
                    report_url = href
                    match_id = self._get_match_id(href)

        if not match_id:
            return None

        # 7) events & predictions
        events = self._get_match_events(table)
        preds = self._get_community_predictions(table)

        # 8) assemble final object
        return MatchContextual(
            match_id=match_id,
            home_team=home_team,
            away_team=away_team,
            final_score=final_score,
            match_report_url=report_url,
            date=metadata.get("date"),
            time=metadata.get("time"),
            day_of_week=metadata.get("day_of_week"),
            referee=metadata.get("referee"),
            attendance=metadata.get("attendance"),
            community_predictions=preds,
            match_events=events,
        )

    def _get_matches(self, soup: BeautifulSoup) -> List[MatchContextual]:
        matches = []

        # find containers containing matches
        match_divs = soup.find_all(MatchItems.DIV, class_=MatchItems.CONTAINER)

        # find match table
        for match_div in match_divs:
            table = match_div.find(MatchItems.BRANCH)
            if table and table.find(MatchItems.TREE, class_=MatchItems.TABLE):
                try:
                    match_table = self._get_match(
                        match_div
                    )  # Fixed: pass match_div, not match_divs
                    if match_table and match_table.match_id:
                        matches.append(match_table)
                except Exception as e:
                    self.logger.warning(f"Failed to extract match: {str(e)}")
                    continue
        return matches

    def _get_matchday_info(
        self, soup: BeautifulSoup, matchday: int, season: str, url: str
    ) -> Dict[str, Any]:
        """Extract matchday info from the page"""
        matchday_info = {"number": matchday, "season": season, "source_url": url}

        # Try to extract actual matchday from page if not provided
        if not matchday:
            matchday_select = soup.find("select", {"name": "spieltag"})
            if matchday_select:
                selected_option = matchday_select.find("option", {"selected": True})
                if selected_option:
                    matchday_text = selected_option.text.strip()
                    matchday_match = re.search(r"(\d+)", matchday_text)
                    if matchday_match:
                        matchday_info["number"] = int(matchday_match.group(1))

        return matchday_info

    def _get_league_table(self, soup: BeautifulSoup) -> Optional[Dict[str, Any]]:
        """Extract league table data from the page - WORKING logic from snippet 3"""
        league_table = {"season": None, "teams": []}

        # Find all boxes and look for the one with league table
        boxes = soup.find_all(MatchItems.DIV, class_=MatchItems.CONTAINER)
        table_box = None

        for box in boxes:
            headline = box.find(MatchItems.DIV, class_=MatchItems.CONTENT_BOX_HEADLINE)
            if headline and (
                MatchItems.TABLE_TEXT in headline.text.lower()
                or MatchItems.PREMIER_LEAGUE_TEXT in headline.text.lower()
            ):
                table_box = box
                break

        if not table_box:
            return None

        # Extract season from headline
        headline = table_box.find(
            MatchItems.DIV, class_=MatchItems.CONTENT_BOX_HEADLINE
        )
        if headline:
            season_text = headline.text.strip()
            season_match = re.search(MatchItems.SEASON_REGEX, season_text)
            if season_match:
                league_table["season"] = season_match.group(1)

        # Find the table
        table = table_box.find(MatchItems.BRANCH)
        if not table:
            return None

        # Extract team data from table rows
        rows = (
            table.find(MatchItems.TBODY).find_all(MatchItems.TREE)
            if table.find(MatchItems.TBODY)
            else []
        )

        for row in rows:
            cells = row.find_all(MatchItems.TREE_DATE)
            if len(cells) < MatchItems.MIN_TABLE_CELLS:
                continue

            # Extract position and movement
            position_cell = cells[MatchItems.POSITION_CELL_IDX]
            position_text = position_cell.text.strip()
            position_match = re.search(MatchItems.POSITION_NUMBER_REGEX, position_text)

            if not position_match:
                continue

            position = int(position_match.group(1))

            # Extract movement indicator
            movement = None
            if MatchItems.GREEN_ARROW in str(position_cell):
                movement = MatchItems.MOVEMENT_UP
            elif MatchItems.RED_ARROW in str(position_cell):
                movement = MatchItems.MOVEMENT_DOWN
            elif MatchItems.GREY_BLOCK in str(position_cell):
                movement = MatchItems.MOVEMENT_SAME

            # Extract team info
            team_link = (
                cells[MatchItems.TEAM_CELL_IDX].find(MatchItems.LINK)
                if len(cells) > MatchItems.TEAM_CELL_IDX
                else None
            )

            # Extract profile_url first
            profile_url = (
                team_link.get(MatchItems.HREF_ATTR, MatchItems.EMPTY_STRING)
                if team_link
                else MatchItems.EMPTY_STRING
            )

            # Extract team_id from profile_url using regex
            team_id = MatchItems.EMPTY_STRING
            if profile_url:
                # Extract from URL like: /fc-energie-cottbus/spielplan/verein/25/saison_id/2024
                team_id_match = re.search(r"/verein/(\d+)/", profile_url)
                if team_id_match:
                    team_id = team_id_match.group(1)

            team_data = {
                "position": position,
                "movement": movement,
                "team_id": team_id,  # ✅ ADD THIS LINE
                "name": (
                    team_link.get(MatchItems.TITLE_ATTR, MatchItems.EMPTY_STRING)
                    .replace(MatchItems.FC_SUFFIX, MatchItems.EMPTY_STRING)
                    .replace(MatchItems.FC_PREFIX, MatchItems.EMPTY_STRING)
                    if team_link
                    else MatchItems.EMPTY_STRING
                ),
                "short_name": (
                    team_link.text.strip() if team_link else MatchItems.EMPTY_STRING
                ),
                "profile_url": profile_url,
                "logo_url": (
                    cells[MatchItems.LOGO_CELL_IDX]
                    .find(MatchItems.IMAGE)
                    .get(MatchItems.SRC_ATTR, MatchItems.EMPTY_STRING)
                    if len(cells) > MatchItems.LOGO_CELL_IDX
                    and cells[MatchItems.LOGO_CELL_IDX].find(MatchItems.IMAGE)
                    else MatchItems.EMPTY_STRING
                ),
                "matches": (
                    int(cells[MatchItems.MATCHES_CELL_IDX].text.strip())
                    if len(cells) > MatchItems.MATCHES_CELL_IDX
                    and cells[MatchItems.MATCHES_CELL_IDX].text.strip().isdigit()
                    else MatchItems.DEFAULT_ZERO
                ),
                "goal_difference": (
                    cells[MatchItems.GOAL_DIFF_CELL_IDX].text.strip()
                    if len(cells) > MatchItems.GOAL_DIFF_CELL_IDX
                    else MatchItems.EMPTY_STRING
                ),
                "points": (
                    int(cells[MatchItems.POINTS_CELL_IDX].text.strip())
                    if len(cells) > MatchItems.POINTS_CELL_IDX
                    and cells[MatchItems.POINTS_CELL_IDX].text.strip().isdigit()
                    else MatchItems.DEFAULT_ZERO
                ),
            }

            league_table["teams"].append(team_data)

        return league_table

    def _get_top_scorers(self, soup: BeautifulSoup) -> Optional[List[Dict[str, Any]]]:
        """Extract top scorers data from the page - WORKING logic from snippet 3"""
        top_scorers = []

        # Find all boxes and look for top goalscorer box
        boxes = soup.find_all(MatchItems.DIV, class_=MatchItems.CONTAINER)
        goalscorer_box = None

        for box in boxes:
            headline = box.find(MatchItems.DIV, class_=MatchItems.CONTENT_BOX_HEADLINE)
            if headline and MatchItems.GOALSCORER_TEXT in headline.text.lower():
                goalscorer_box = box
                break

        if not goalscorer_box:
            return None

        # Find the table
        table = goalscorer_box.find(MatchItems.BRANCH)
        if not table:
            return None

        # Extract scorer data from table rows
        rows = (
            table.find(MatchItems.TBODY).find_all(MatchItems.TREE)
            if table.find(MatchItems.TBODY)
            else []
        )

        for row in rows:
            cells = row.find_all(MatchItems.TREE_DATE)
            if len(cells) < MatchItems.MIN_SCORER_CELLS:
                continue

            # Extract player info
            player_cell = cells[MatchItems.PLAYER_CELL_IDX]
            full_name_span = player_cell.find(
                MatchItems.SPAN, class_=MatchItems.HIDE_FOR_SMALL_CLASS
            )
            short_name_span = player_cell.find(
                MatchItems.SPAN, class_=MatchItems.SHOW_FOR_SMALL_CLASS
            )

            player_link = (
                full_name_span.find(MatchItems.LINK)
                if full_name_span
                else short_name_span.find(MatchItems.LINK)
                if short_name_span
                else None
            )

            if not player_link:
                continue

            # Extract club info
            club_cell = cells[MatchItems.CLUB_CELL_IDX]
            club_images = club_cell.find_all(MatchItems.IMAGE)
            clubs = []

            for img in club_images:
                clubs.append(
                    {
                        "name": img.get(MatchItems.TITLE_ATTR, MatchItems.EMPTY_STRING),
                        "logo_url": img.get(
                            MatchItems.SRC_ATTR, MatchItems.EMPTY_STRING
                        ),
                    }
                )

            # Extract goals
            matchday_goals = cells[MatchItems.MATCHDAY_GOALS_CELL_IDX].text.strip()
            total_goals = cells[MatchItems.TOTAL_GOALS_CELL_IDX].text.strip()

            scorer_data = {
                "name": player_link.get(
                    MatchItems.TITLE_ATTR, player_link.text.strip()
                ),
                "short_name": (
                    short_name_span.find(MatchItems.LINK).text.strip()
                    if short_name_span and short_name_span.find(MatchItems.LINK)
                    else MatchItems.EMPTY_STRING
                ),
                "profile_url": player_link.get(
                    MatchItems.HREF_ATTR, MatchItems.EMPTY_STRING
                ),
                "clubs": clubs,
                "goals_this_matchday": (
                    matchday_goals
                    if matchday_goals != MatchItems.DASH
                    else MatchItems.DEFAULT_ZERO
                ),
                "total_goals": (
                    int(total_goals)
                    if total_goals.isdigit()
                    else MatchItems.DEFAULT_ZERO
                ),
            }

            top_scorers.append(scorer_data)

        return top_scorers

    def _get_matchday_summary(self, soup: BeautifulSoup) -> Optional[Dict[str, Any]]:
        """Extract matchday summary statistics - WORKING logic from snippet 3"""
        summary = {}

        # Find all boxes and look for matchday summary box
        boxes = soup.find_all(MatchItems.DIV, class_=MatchItems.CONTAINER)
        summary_box = None

        for box in boxes:
            headline = box.find(MatchItems.H2, class_=MatchItems.CONTENT_BOX_HEADLINE)
            if headline and MatchItems.SUMMARY_TEXT in headline.text.lower():
                summary_box = box
                break

        if not summary_box:
            return None

        # Find the table
        table = summary_box.find(MatchItems.BRANCH)
        if not table:
            return None

        # Extract data from both body and footer
        tbody = table.find(MatchItems.TBODY)
        tfoot = table.find("tfoot")

        # Extract current matchday data (tbody)
        if tbody:
            main_row = tbody.find(MatchItems.TREE)
            if main_row:
                cells = main_row.find_all(MatchItems.TREE_DATE)

                if len(cells) >= MatchItems.MIN_SUMMARY_CELLS:
                    summary["current_matchday"] = {
                        "matches": (
                            int(cells[MatchItems.SUMMARY_MATCHES_IDX].text.strip())
                            if cells[MatchItems.SUMMARY_MATCHES_IDX]
                            .text.strip()
                            .isdigit()
                            else MatchItems.DEFAULT_ZERO
                        ),
                        "goals": (
                            int(cells[MatchItems.SUMMARY_GOALS_IDX].text.strip())
                            if cells[MatchItems.SUMMARY_GOALS_IDX]
                            .text.strip()
                            .isdigit()
                            else MatchItems.DEFAULT_ZERO
                        ),
                        "own_goals": (
                            int(cells[MatchItems.SUMMARY_OWN_GOALS_IDX].text.strip())
                            if cells[MatchItems.SUMMARY_OWN_GOALS_IDX]
                            .text.strip()
                            .isdigit()
                            else MatchItems.DEFAULT_ZERO
                        ),
                        "yellow_cards": (
                            int(cells[MatchItems.SUMMARY_YELLOW_CARDS_IDX].text.strip())
                            if cells[MatchItems.SUMMARY_YELLOW_CARDS_IDX]
                            .text.strip()
                            .isdigit()
                            else MatchItems.DEFAULT_ZERO
                        ),
                        "second_yellow_cards": cells[
                            MatchItems.SUMMARY_SECOND_YELLOW_IDX
                        ].text.strip(),
                        "red_cards": (
                            int(cells[MatchItems.SUMMARY_RED_CARDS_IDX].text.strip())
                            if cells[MatchItems.SUMMARY_RED_CARDS_IDX]
                            .text.strip()
                            .isdigit()
                            else MatchItems.DEFAULT_ZERO
                        ),
                        "total_attendance": (
                            cells[MatchItems.SUMMARY_TOTAL_ATTENDANCE_IDX]
                            .text.strip()
                            .replace(MatchItems.DOT_SEPARATOR, MatchItems.EMPTY_STRING)
                            .replace(
                                MatchItems.COMMA_SEPARATOR, MatchItems.EMPTY_STRING
                            )
                            if len(cells) > MatchItems.SUMMARY_TOTAL_ATTENDANCE_IDX
                            else MatchItems.EMPTY_STRING
                        ),
                        "average_attendance": (
                            cells[MatchItems.SUMMARY_AVG_ATTENDANCE_IDX]
                            .text.strip()
                            .replace(MatchItems.DOT_SEPARATOR, MatchItems.EMPTY_STRING)
                            .replace(
                                MatchItems.COMMA_SEPARATOR, MatchItems.EMPTY_STRING
                            )
                            if len(cells) > MatchItems.SUMMARY_AVG_ATTENDANCE_IDX
                            else MatchItems.EMPTY_STRING
                        ),
                        "sold_out_matches": (
                            int(cells[MatchItems.SUMMARY_SOLD_OUT_IDX].text.strip())
                            if len(cells) > MatchItems.SUMMARY_SOLD_OUT_IDX
                            and cells[MatchItems.SUMMARY_SOLD_OUT_IDX]
                            .text.strip()
                            .isdigit()
                            else MatchItems.DEFAULT_ZERO
                        ),
                    }

        return summary if summary else None

    def extract_matchday(
        self, soup: BeautifulSoup, matchday, season
    ) -> MatchdayContainer:
        """
        This extracts all the matchday contextual information:
        """

        matches = self._get_matches(soup)
        league_table = self._get_league_table(soup)
        top_scorers = self._get_top_scorers(soup)
        matchday_summary = self._get_matchday_summary(soup)

        # Extract matchday info from the page
        matchday_info = self._get_matchday_info(soup, matchday, season, url="")

        return MatchdayContainer(
            matchday_info=matchday_info,
            league_table=league_table,
            top_scorers=top_scorers,
            matchday_summary=matchday_summary,
            matches=matches,
            metadata={
                "extraction_time": datetime.now().isoformat(),
                "source": "matchday_page",
                "url": "",
                "total_matches": len(matches),
            },
        )


class MatchExtractor:
    """Extracts detailed match data using the WORKING notebook snippet 2 logic"""

    def __init__(self):
        self.position_mappings = {
            "GK": "Goalkeeper",
            "CB": "Centre Back",
            "LB": "Left Back",
            "RB": "Right Back",
            "LWB": "Left Wing Back",
            "RWB": "Right Wing Back",
            "DM": "Defensive Midfielder",
            "CM": "Central Midfielder",
            "AM": "Attacking Midfielder",
            "LM": "Left Midfielder",
            "RM": "Right Midfielder",
            "LW": "Left Winger",
            "RW": "Right Winger",
            "CF": "Centre Forward",
            "ST": "Striker",
        }

    @backoff.on_exception(backoff.expo, Exception, max_tries=3)
    def extract_from_url(self, soup, url: str) -> MatchDetail:
        """Extract detailed match data using the WORKING logic from notebook snippet 2"""

        # Extract match ID
        match_id = self._extract_match_id(soup, url)

        # Extract all components using WORKING logic from snippet 2
        match_info = self._extract_match_info(soup, match_id)
        home_team, away_team = self._extract_teams(soup)
        score = self._extract_score(soup)
        lineups = self._extract_lineups(soup)
        goals = self._extract_goals(soup)
        cards = self._extract_cards(soup)
        substitutions = self._extract_substitutions(soup)
        all_players = self._create_player_lookup(
            lineups["home_starting"],
            lineups["away_starting"],
            lineups["home_subs"],
            lineups["away_subs"],
        )

        goals = self._backfill_shirt_numbers_in_goals(goals, all_players)
        cards = self._backfill_shirt_numbers_in_cards(cards, all_players)
        substitutions = self._backfill_shirt_numbers_in_substitutions(
            substitutions, all_players
        )

        return MatchDetail(
            match_info=match_info,
            home_team=home_team,
            away_team=away_team,
            score=score,
            home_lineup=lineups["home_starting"],
            away_lineup=lineups["away_starting"],
            home_substitutes=lineups["home_subs"],
            away_substitutes=lineups["away_subs"],
            goals=goals,
            cards=cards,
            substitutions=substitutions,
            extraction_metadata={
                "extraction_time": datetime.now().isoformat(),
                "extractor_version": "1.0.0",
                "source": "match_report_page",
                "source_url": url,
            },
        )

    def _create_player_lookup(
        self,
        home_starting: List[Player],
        away_starting: List[Player],
        home_subs: List[Player],
        away_subs: List[Player],
    ) -> Dict[str, Player]:
        """Create lookup dictionary by player_id for fast matching"""
        all_players = {}

        for player_list in [home_starting, away_starting, home_subs, away_subs]:
            for player in player_list:
                if player.player_id:
                    all_players[player.player_id] = player

        return all_players

    def _backfill_shirt_numbers_in_goals(
        self, goals: List[Goal], player_lookup: Dict[str, Player]
    ) -> List[Goal]:
        """Backfill shirt numbers in goal events from lineup data"""
        updated_goals = []

        for goal in goals:
            updated_goal = Goal(
                minute=goal.minute,
                extra_time=goal.extra_time,
                player=self._add_shirt_number_to_player(goal.player, player_lookup),
                assist_player=self._add_shirt_number_to_player(
                    goal.assist_player, player_lookup
                ),
                goal_type=goal.goal_type,
                assist_type=goal.assist_type,
                team_side=goal.team_side,
                score_after=goal.score_after,
                season_goal_number=goal.season_goal_number,
                season_assist_number=goal.season_assist_number,
            )
            updated_goals.append(updated_goal)

        return updated_goals

    def _backfill_shirt_numbers_in_cards(
        self, cards: List[Card], player_lookup: Dict[str, Player]
    ) -> List[Card]:
        """Backfill shirt numbers in card events from lineup data"""
        updated_cards = []

        for card in cards:
            updated_card = Card(
                minute=card.minute,
                extra_time=card.extra_time,
                player=self._add_shirt_number_to_player(card.player, player_lookup),
                card_type=card.card_type,
                reason=card.reason,
                team_side=card.team_side,
                season_card_number=card.season_card_number,
            )
            updated_cards.append(updated_card)

        return updated_cards

    def _backfill_shirt_numbers_in_substitutions(
        self, substitutions: List[Substitution], player_lookup: Dict[str, Player]
    ) -> List[Substitution]:
        """Backfill shirt numbers in substitution events from lineup data"""
        updated_substitutions = []

        for sub in substitutions:
            updated_sub = Substitution(
                minute=sub.minute,
                extra_time=sub.extra_time,
                player_out=self._add_shirt_number_to_player(
                    sub.player_out, player_lookup
                ),
                player_in=self._add_shirt_number_to_player(
                    sub.player_in, player_lookup
                ),
                reason=sub.reason,
                team_side=sub.team_side,
            )
            updated_substitutions.append(updated_sub)

        return updated_substitutions

    def _add_shirt_number_to_player(
        self, player: Optional[Player], player_lookup: Dict[str, Player]
    ) -> Optional[Player]:
        """Add shirt number to player from lookup if available"""
        if not player or not player.player_id:
            return player

        # Find matching player in lookup
        lineup_player = player_lookup.get(player.player_id)
        if lineup_player and lineup_player.shirt_number is not None:
            # Create new player with shirt number
            return Player(
                player_id=player.player_id,
                name=player.name,
                shirt_number=lineup_player.shirt_number,  # Backfilled from lineup!
                position=player.position,
                is_captain=player.is_captain,
                portrait_url=player.portrait_url,
            )

        return player  # Return original if no match found

    def _extract_match_id(self, soup: BeautifulSoup, url: str) -> str:
        """Extract match ID - WORKING logic from snippet 2"""
        # Try URL first
        match = re.search(r"/spielbericht/(\d+)", url)
        if match:
            return match.group(1)

        # Try meta property
        meta_url = soup.find("meta", property="og:url")
        if meta_url:
            match = re.search(r"spielbericht/(\d+)", meta_url.get("content", ""))
            if match:
                return match.group(1)

        # Try canonical URL
        canonical = soup.find("link", rel="canonical")
        if canonical:
            match = re.search(r"spielbericht/(\d+)", canonical.get("href", ""))
            if match:
                return match.group(1)

        return str(uuid.uuid4())[:8]  # Fallback

    def _extract_match_info(self, soup: BeautifulSoup, match_id: str) -> MatchInfo:
        """Extract basic match info - WORKING logic from snippet 2"""

        # Competition info
        comp_link = soup.find("a", class_="direct-headline__link")
        competition_name = comp_link.text.strip() if comp_link else ""
        competition_id = (
            self._extract_id_from_href(comp_link.get("href", "")) if comp_link else None
        )

        # Matchday
        matchday_link = soup.find("a", href=re.compile(r"jumplist/spieltag"))
        matchday_text = matchday_link.text if matchday_link else ""
        matchday = self._extract_number_from_text(matchday_text)

        # Date and time
        date_link = soup.find("a", href=re.compile(r"waspassiertheute"))
        date_str = date_link.text.strip() if date_link else ""
        date = self._parse_date(date_str)

        # Time
        time_match = re.search(r"(\d{1,2}:\d{2})\s*(AM|PM)?", soup.get_text())
        time = time_match.group(1) if time_match else None

        # Venue
        venue_link = soup.find("a", href=re.compile(r"/stadion/"))
        venue = venue_link.text.strip() if venue_link else None

        # Attendance
        attendance = self._extract_attendance(soup)

        # Referee
        referee_link = soup.find("a", href=re.compile(r"/profil/schiedsrichter/"))
        referee = referee_link.text.strip() if referee_link else None
        referee_id = (
            self._extract_id_from_href(referee_link.get("href", ""))
            if referee_link
            else None
        )

        return MatchInfo(
            match_id=match_id,
            competition_name=competition_name,
            competition_id=competition_id,
            matchday=matchday,
            date=date,
            time=time,
            venue=venue,
            attendance=attendance,
            referee=referee,
            referee_id=referee_id,
        )

    def _extract_teams(self, soup: BeautifulSoup) -> Tuple[Team, Team]:
        """Extract both teams - WORKING logic from snippet 2"""

        # Home team
        home_section = soup.select_one("div.box-content div.sb-team.sb-heim")
        home_team = self._extract_single_team(home_section, soup, "home")

        # Away team
        away_section = soup.select_one("div.box-content div.sb-team.sb-gast")
        away_team = self._extract_single_team(away_section, soup, "away")

        return home_team, away_team

    def _extract_single_team(
        self, section: Tag, soup: BeautifulSoup, team_type: str
    ) -> Team:
        """Extract single team - UPDATED with universal manager extraction"""
        if not section:
            return Team(team_id="", name="")

        team_link = section.find("a", class_="sb-vereinslink")
        team_logo = section.find("img")
        position_elem = section.find("p")

        return Team(
            team_id=(
                self._extract_id_from_href(team_link.get("href", ""))
                if team_link
                else ""
            ),
            name=team_link.text.strip() if team_link else "",
            logo_url=team_logo.get("src") if team_logo else None,
            league_position=self._extract_league_position(
                position_elem.text if position_elem else ""
            ),
            formation=self._extract_formation(soup, team_type),
            manager=self._extract_manager(soup, team_type),
        )

    def _extract_score(self, soup: BeautifulSoup) -> Score:
        """Extract match score - WORKING logic from snippet 2"""
        score_elem = soup.find("div", class_="sb-endstand")

        if not score_elem:
            return Score(home_final=0, away_final=0)

        # Extract final score
        score_text = score_elem.text.strip()
        final_match = re.search(r"(\d+):(\d+)", score_text.split("(")[0])

        home_final = int(final_match.group(1)) if final_match else 0
        away_final = int(final_match.group(2)) if final_match else 0

        # Extract half-time score
        ht_elem = score_elem.find("div", class_="sb-halbzeit")
        home_ht = away_ht = None

        if ht_elem:
            ht_text = ht_elem.text.strip("()")
            ht_match = re.search(r"(\d+):(\d+)", ht_text)
            if ht_match:
                home_ht = int(ht_match.group(1))
                away_ht = int(ht_match.group(2))

        return Score(
            home_final=home_final,
            away_final=away_final,
            home_ht=home_ht,
            away_ht=away_ht,
        )

    def _extract_lineups(self, soup: BeautifulSoup) -> Dict[str, List[Player]]:
        """Extract complete lineups - UNIVERSAL logic for both formats"""
        lineups = {
            "home_starting": [],
            "away_starting": [],
            "home_subs": [],
            "away_subs": [],
        }

        if self._has_formation_layout(soup):
            return self._extract_formation_lineups(soup)
        else:
            return self._extract_simple_table_lineups(soup)

    def _has_formation_layout(self, soup: BeautifulSoup) -> bool:
        """Check if HTML contains formation-based layout elements"""
        formation_players = soup.find_all("div", class_="formation-player-container")
        subs_tables = soup.find_all("table", class_="ersatzbank")
        return len(formation_players) > 0 and len(subs_tables) > 0

    def _extract_formation_lineups(
        self, soup: BeautifulSoup
    ) -> Dict[str, List[Player]]:
        """Extract lineups from formation-based layout (like Man United vs Fulham)"""
        lineups = {
            "home_starting": [],
            "away_starting": [],
            "home_subs": [],
            "away_subs": [],
        }

        # Get team containers - look for the main lineup sections
        lineup_sections = soup.find_all("div", class_="large-6")

        if len(lineup_sections) >= 2:
            # Home team (first section)
            home_section = lineup_sections[0]
            lineups["home_starting"] = self._extract_starting_xi(home_section)
            lineups["home_subs"] = self._extract_substitutes(home_section)

            # Away team (second section)
            away_section = lineup_sections[1]
            lineups["away_starting"] = self._extract_starting_xi(away_section)
            lineups["away_subs"] = self._extract_substitutes(away_section)

        return lineups

    def _extract_simple_table_lineups(
        self, soup: BeautifulSoup
    ) -> Dict[str, List[Player]]:
        """Extract lineups from simple table layout (like RFCU Kelmis vs RFC Raeren-Eynatten)"""
        lineups = {
            "home_starting": [],
            "away_starting": [],
            "home_subs": [],
            "away_subs": [],
        }

        # Get team containers - look for the main lineup sections
        lineup_sections = soup.find_all("div", class_="large-6")

        for idx, section in enumerate(lineup_sections):
            is_home = idx == 0
            starters = []

            # Find the main table with player positions
            table = section.find("table")
            if not table:
                continue

            for tr in table.find_all("tr"):
                cells = tr.find_all("td")
                if len(cells) != 2:
                    continue

                position_cell = cells[0]
                players_cell = cells[1]

                # Get position (skip manager)
                position_elem = position_cell.find("b")
                if not position_elem:
                    continue

                position = position_elem.get_text(strip=True)
                if position.lower() == "manager":
                    continue

                # Get all players in this position
                for a in players_cell.find_all("a"):
                    player = Player(
                        player_id=self._extract_id_from_href(a.get("href", "")),
                        name=a.get_text(strip=True),
                        position=position,
                        shirt_number=None,  # Simple table doesn't show numbers
                        is_captain=False,  # No captain info in simple format
                        portrait_url=self._extract_player_portrait_url(
                            a.get("href", "")
                        ),
                    )
                    starters.append(player)

            # Assign to home or away
            if is_home:
                lineups["home_starting"] = starters
            else:
                lineups["away_starting"] = starters

        return lineups

    def _extract_starting_xi(self, section) -> List[Player]:
        """
        Extract starting XI
        """
        players = []

        # First try formation-based extraction
        formation_players = section.find_all("div", class_="formation-player-container")

        if formation_players:
            # Formation-based layout
            for player_container in formation_players:
                # Find shirt number
                shirt_number_elem = player_container.find(
                    "div", class_="tm-shirt-number"
                )
                shirt_number = None
                if shirt_number_elem:
                    shirt_text = shirt_number_elem.get_text(strip=True)
                    if shirt_text.isdigit():
                        shirt_number = int(shirt_text)

                # Find player link
                player_link = player_container.find("a")

                # Find captain icon
                captain_elem = player_container.find(
                    "div", class_="kapitaenicon-formation"
                )

                if player_link:
                    player = Player(
                        player_id=self._extract_id_from_href(
                            player_link.get("href", "")
                        ),
                        name=player_link.text.strip(),
                        shirt_number=shirt_number,
                        position=None,  # Position not available in formation view
                        is_captain=captain_elem is not None,
                        portrait_url=self._extract_player_portrait_url(
                            player_link.get("href", "")
                        ),
                    )
                    players.append(player)

        return players

    def _extract_substitutes(self, section: Tag) -> List[Player]:
        """Extract substitute players - WORKING logic from snippet 2"""
        players = []
        bench_table = section.find("table", class_="ersatzbank")

        if not bench_table:
            return players

        rows = bench_table.find_all("tr")
        for row in rows:
            if "Manager:" in row.get_text():
                continue

            cells = row.find_all("td")
            if len(cells) >= 3:
                number_elem = cells[0].find("div", class_="tm-shirt-number")
                player_link = cells[1].find("a")
                position_cell = cells[2]

                if player_link:
                    player = Player(
                        player_id=self._extract_id_from_href(
                            player_link.get("href", "")
                        ),
                        name=player_link.text.strip(),
                        shirt_number=self._extract_shirt_number(
                            number_elem.text if number_elem else ""
                        ),
                        position=position_cell.text.strip() if position_cell else None,
                        portrait_url=self._extract_player_portrait_url(
                            player_link.get("href", "")
                        ),
                    )
                    players.append(player)

        return players

    def _extract_goals(self, soup: BeautifulSoup) -> List[Goal]:
        """Extract all goal events - WORKING logic from snippet 2"""
        goals = []
        goals_section = soup.find("div", id="sb-tore")

        if not goals_section:
            return goals

        goal_items = goals_section.find_all("li")

        for item in goal_items:
            try:
                goal = self._parse_goal_event(item)
                if goal:
                    goals.append(goal)
            except Exception as e:
                self.logger.warning(f"Failed to parse goal event: {str(e)}")
                continue

        return goals

    def _extract_cards(self, soup: BeautifulSoup) -> List[Card]:
        """Extract all card events - WORKING logic from snippet 2"""
        cards = []
        cards_section = soup.find("div", id="sb-karten")

        if not cards_section:
            return cards

        card_items = cards_section.find_all("li")

        for item in card_items:
            try:
                card = self._parse_card_event(item)
                if card:
                    cards.append(card)
            except Exception as e:
                self.logger.warning(f"Failed to parse card event: {str(e)}")
                continue

        return cards

    def _parse_card_event(self, item: Tag) -> Optional[Card]:
        """Parse a single card event - WORKING logic from snippet 2"""
        team_side = "home" if "sb-aktion-heim" in item.get("class", []) else "away"
        minute, extra_time = self._extract_minute_from_event(item)

        # Determine card type
        card_type = "yellow"
        if item.find("span", class_="sb-rot"):
            card_type = "red"
        elif item.find("span", class_="sb-gelbrot"):
            card_type = "second_yellow"

        # Extract player
        player_link = item.find("a", class_="wichtig")
        player = None
        if player_link:
            player = Player(
                player_id=self._extract_id_from_href(player_link.get("href", "")),
                name=player_link.text.strip(),
                portrait_url=self._extract_player_portrait_url(
                    player_link.get("href", "")
                ),
            )

        # Extract reason and season number
        description = item.find("div", class_="sb-aktion-aktion")
        reason, season_card_number = self._parse_card_description(
            description.text if description else ""
        )

        return Card(
            minute=minute,
            extra_time=extra_time,
            player=player,
            card_type=card_type,
            reason=reason,
            team_side=team_side,
            season_card_number=season_card_number,
        )

    def _extract_substitutions(self, soup: BeautifulSoup) -> List[Substitution]:
        """Extract all substitution events - WORKING logic from snippet 2"""
        substitutions = []
        subs_section = soup.find("div", id="sb-wechsel")

        if not subs_section:
            return substitutions

        sub_items = subs_section.find_all("li")

        for item in sub_items:
            try:
                substitution = self._parse_substitution_event(item)
                if substitution:
                    substitutions.append(substitution)
            except Exception as e:
                self.logger.warning(f"Failed to parse substitution event: {str(e)}")
                continue

        return substitutions

    def _parse_substitution_event(self, item: Tag) -> Optional[Substitution]:
        """Parse a single substitution event - WORKING logic from snippet 2"""
        team_side = "home" if "sb-aktion-heim" in item.get("class", []) else "away"
        minute, extra_time = self._extract_minute_from_event(item)

        # Extract players using improved method
        player_out, player_in = self._extract_substitution_players(item)

        # Extract reason
        reason = self._extract_substitution_reason(item)

        return Substitution(
            minute=minute,
            extra_time=extra_time,
            player_out=player_out,
            player_in=player_in,
            reason=reason,
            team_side=team_side,
        )

    def _extract_substitution_players(
        self, item: Tag
    ) -> Tuple[Optional[Player], Optional[Player]]:
        """Extract both players in substitution - WORKING logic from snippet 2"""
        player_out = player_in = None

        # Method 1: Look for specific span classes
        player_out_section = item.find("span", class_="sb-aktion-wechsel-aus")
        if player_out_section:
            player_out_link = player_out_section.find("a", class_="wichtig")
            if player_out_link:
                player_out = Player(
                    player_id=self._extract_id_from_href(
                        player_out_link.get("href", "")
                    ),
                    name=player_out_link.text.strip(),
                    portrait_url=self._extract_player_portrait_url(
                        player_out_link.get("href", "")
                    ),
                )

        player_in_section = item.find("span", class_="sb-aktion-wechsel-ein")
        if player_in_section:
            player_in_link = player_in_section.find("a", class_="wichtig")
            if player_in_link:
                player_in = Player(
                    player_id=self._extract_id_from_href(
                        player_in_link.get("href", "")
                    ),
                    name=player_in_link.text.strip(),
                    portrait_url=self._extract_player_portrait_url(
                        player_in_link.get("href", "")
                    ),
                )

        # Method 2: Fallback - look for all player links
        if not player_out or not player_in:
            all_player_links = item.find_all("a", class_="wichtig")
            if len(all_player_links) >= 2:
                # Usually first is out, second is in
                if not player_out:
                    player_out = Player(
                        player_id=self._extract_id_from_href(
                            all_player_links[0].get("href", "")
                        ),
                        name=all_player_links[0].text.strip(),
                        portrait_url=self._extract_player_portrait_url(
                            all_player_links[0].get("href", "")
                        ),
                    )
                if not player_in:
                    player_in = Player(
                        player_id=self._extract_id_from_href(
                            all_player_links[1].get("href", "")
                        ),
                        name=all_player_links[1].text.strip(),
                        portrait_url=self._extract_player_portrait_url(
                            all_player_links[1].get("href", "")
                        ),
                    )

        return player_out, player_in

    def _extract_substitution_reason(self, item: Tag) -> Optional[str]:
        """Extract substitution reason - WORKING logic from snippet 2"""
        # Look for reason span
        reason_elem = item.find("span", class_=re.compile(r"sb-wechsel-\d+"))
        if reason_elem:
            if "sb-wechsel-402" in reason_elem.get("class", []):
                return "Injury"
            elif "sb-wechsel-401" in reason_elem.get("class", []):
                return "Tactical"

        # Check text content
        item_text = item.get_text()
        if "Injury" in item_text:
            return "Injury"
        elif "Tactical" in item_text:
            return "Tactical"

        return None

    def _extract_minute_from_event(self, item: Tag) -> Tuple[int, Optional[int]]:
        """Extract minute from event - FINAL CORRECTED VERSION"""

        # Find the clock element with minute information
        clock_elem = item.find("span", class_=re.compile(r"sb-sprite-uhr"))
        if clock_elem:
            style = clock_elem.get("style", "")

            # Check for extra time display in text content first
            clock_text = clock_elem.get_text(strip=True)
            if "+" in clock_text:
                try:
                    extra_time = int(clock_text.replace("+", ""))
                    return 90, extra_time
                except:
                    pass
            elif clock_text.isdigit():
                return int(clock_text), None

            # Parse minute from CSS background-position (BOTH X and Y)
            if "background-position:" in style:
                # Extract BOTH x and y positions
                pattern = r"background-position:\s*-?(\d+)px\s+-?(\d+)px"
                match = re.search(pattern, style)
                if match:
                    x_pos = int(match.group(1))
                    y_pos = int(match.group(2))

                    # Calculate minute using CONFIRMED formula
                    # Based on verified examples:
                    # 4 min: x=108, y=0   -> x_grid=3, y_grid=0 -> minute = 3 + (0*10) + 1 = 4 ✓
                    # 36 min: x=180, y=108 -> x_grid=5, y_grid=3 -> minute = 5 + (3*10) + 1 = 36 ✓
                    # 75 min: x=144, y=252 -> x_grid=4, y_grid=7 -> minute = 4 + (7*10) + 1 = 75 ✓

                    x_grid = x_pos // 36
                    y_grid = y_pos // 36

                    # CONFIRMED FORMULA: 10 columns per row layout
                    minute = x_grid + (y_grid * 10) + 1

                    # Sanity check for reasonable minute values
                    if 1 <= minute <= 120:  # Allow up to 120 for extra time
                        return minute, None

                    # Fallback to simple x calculation
                    if x_grid > 0:
                        return min(x_grid + 1, 90), None

        # Look for specific minute patterns in the item text
        item_text = item.get_text()
        minute_patterns = [
            r"(\d+)\'",  # 4'
            r"(\d+)\s*min",  # 4 min
            r"(\d+)\.",  # 4.
        ]

        for pattern in minute_patterns:
            match = re.search(pattern, item_text)
            if match:
                minute = int(match.group(1))
                if minute <= 90:
                    return minute, None

        # Fallback: estimate from list position
        return self._estimate_minute_from_list_position(item), None

    def _estimate_minute_from_list_position(self, item: Tag) -> int:
        """Estimate minute based on position in event list - IMPROVED"""
        parent_list = item.find_parent("ul")
        if parent_list:
            items = parent_list.find_all("li")
            try:
                index = items.index(item)
                # More realistic distribution: events typically happen throughout the game
                # First half: 0-45, Second half: 45-90
                if index == 0:
                    return 1  # First event often early
                elif index < len(items) // 2:
                    # First half events
                    return min(5 + (index * 8), 45)
                else:
                    # Second half events
                    return min(50 + ((index - len(items) // 2) * 10), 90)
            except:
                pass
        return 45  # Default fallback

    # Helper methods from snippet 2
    def _extract_attendance(self, soup: BeautifulSoup) -> Optional[int]:
        """Extract attendance - WORKING logic from snippet 2"""
        attendance_elem = soup.find("strong", string=re.compile(r"Attendance:"))
        if attendance_elem and attendance_elem.parent:
            attendance_text = attendance_elem.parent.text
            attendance_match = re.search(r"Attendance:\s*([\d,\.]+)", attendance_text)
            if attendance_match:
                return int(attendance_match.group(1).replace(",", "").replace(".", ""))
        return None

    def _extract_formation(self, soup: BeautifulSoup, team: str) -> Optional[str]:
        """Extract team formation - WORKING logic from snippet 2"""
        formation_elems = soup.find_all("div", class_="formation-subtitle")

        if not formation_elems:
            return None

        formation_index = 0 if team == "home" else 1

        if len(formation_elems) > formation_index:
            formation_elem = formation_elems[formation_index]
            formation_text = formation_elem.get_text()

            formation_match = re.search(r"Starting Line-up:\s*(.+)", formation_text)
            if formation_match:
                full_formation = formation_match.group(1).strip()
                return full_formation

            numeric_match = re.search(r"(\d+-\d+-\d+(?:-\d+)?)", formation_text)
            if numeric_match:
                return numeric_match.group(1)

        return None

    def _extract_manager(self, soup: BeautifulSoup, team: str) -> Optional[str]:
        """Extract team manager - UNIVERSAL logic for both formats"""

        # Method 1: Try formation layout first (ersatzbank tables)
        tables = soup.find_all("table", class_="ersatzbank")
        table_index = 0 if team == "home" else 1

        if len(tables) > table_index:
            manager_row = tables[table_index].find("tr", class_="bench-table__tr")
            if manager_row:
                manager_link = manager_row.find("a")
                if manager_link:
                    return manager_link.text.strip()

        # Method 2: Try simple table layout (main lineup tables)
        lineup_sections = soup.find_all("div", class_="large-6")
        section_index = 0 if team == "home" else 1

        if len(lineup_sections) > section_index:
            section = lineup_sections[section_index]
            table = section.find("table")

            if table:
                # Look for manager row in the main table
                for tr in table.find_all("tr"):
                    cells = tr.find_all("td")
                    if len(cells) == 2:
                        position_cell = cells[0]
                        manager_cell = cells[1]

                        # Check if this is the manager row
                        position_elem = position_cell.find("b")
                        if (
                            position_elem
                            and position_elem.get_text(strip=True).lower() == "manager"
                        ):
                            manager_link = manager_cell.find("a")
                            if manager_link:
                                return manager_link.text.strip()

        return None

    def _parse_goal_description(
        self, text: str
    ) -> Tuple[Optional[Player], Optional[str], Dict[str, int], Optional[str]]:
        """Parse goal description - IMPROVED to extract goal type and assist type dynamically"""
        assist_player = None
        goal_type = None
        assist_type = None
        season_numbers = {}

        # Extract goal type dynamically (between player name and season info)
        goal_type_match = re.search(
            r"</a>,\s*([^,]+),\s*\d+\.\s*Goal of the Season", text
        )
        if goal_type_match:
            goal_type = goal_type_match.group(1).strip()

        # Extract assist player with proper ID extraction and assist type
        assist_match = re.search(
            r'Assist:\s*<a[^>]*href="[^"]*spieler/(\d+)/[^"]*"[^>]*>([^<]+)</a>,\s*([^,]+),\s*\d+\.\s*Assist of the Season',
            text,
        )
        if assist_match:
            assist_player_id = assist_match.group(1)
            assist_name = assist_match.group(2).strip()
            assist_type = assist_match.group(3).strip()  # Extract assist type!
            assist_player = Player(
                player_id=assist_player_id,
                name=assist_name,
                portrait_url=self._extract_player_portrait_url(
                    f"/spieler/{assist_player_id}"
                ),
            )
        else:
            # Fallback: try simpler assist extraction without type
            assist_match = re.search(
                r'Assist:\s*<a[^>]*href="[^"]*spieler/(\d+)/[^"]*"[^>]*>([^<]+)</a>',
                text,
            )
            if assist_match:
                assist_player_id = assist_match.group(1)
                assist_name = assist_match.group(2).strip()
                assist_player = Player(
                    player_id=assist_player_id,
                    name=assist_name,
                    portrait_url=self._extract_player_portrait_url(
                        f"/spieler/{assist_player_id}"
                    ),
                )
            else:
                # Final fallback: your original method
                assist_match = re.search(r"Assist:\s*([^,]+)", text)
                if assist_match:
                    assist_name = assist_match.group(1).strip()
                    assist_player = Player(player_id="", name=assist_name)

        # Extract season numbers
        goal_number_match = re.search(r"(\d+)\.\s*Goal of the Season", text)
        if goal_number_match:
            season_numbers["goals"] = int(goal_number_match.group(1))

        assist_number_match = re.search(r"(\d+)\.\s*Assist of the Season", text)
        if assist_number_match:
            season_numbers["assists"] = int(assist_number_match.group(1))

        return assist_player, goal_type, season_numbers, assist_type

    def _parse_goal_event(self, item: Tag) -> Optional[Goal]:
        """Parse a single goal event - UPDATED to use better assist extraction"""
        team_side = "home" if "sb-aktion-heim" in item.get("class", []) else "away"
        minute, extra_time = self._extract_minute_from_event(item)

        # Extract score after goal
        score_elem = item.find("div", class_="sb-aktion-spielstand")
        score_after = self._parse_score_from_text(score_elem.text if score_elem else "")

        # Extract scorer
        scorer_link = item.find("a", class_="wichtig")
        scorer = None
        if scorer_link:
            scorer = Player(
                player_id=self._extract_id_from_href(scorer_link.get("href", "")),
                name=scorer_link.text.strip(),
                portrait_url=self._extract_player_portrait_url(
                    scorer_link.get("href", "")
                ),
            )

        # Extract assist and goal details from the action div (which contains HTML)
        description_elem = item.find("div", class_="sb-aktion-aktion")
        if description_elem:
            # Use the HTML content for better parsing
            description_html = str(description_elem)
            assist_player, goal_type, season_numbers, assist_type = (
                self._parse_goal_description(description_html)
            )
        else:
            assist_player, goal_type, season_numbers = None, None, {}

        return Goal(
            minute=minute,
            extra_time=extra_time,
            player=scorer,
            assist_player=assist_player,
            goal_type=goal_type,
            assist_type=assist_type,
            team_side=team_side,
            score_after=score_after,
            season_goal_number=season_numbers.get("goals"),
            season_assist_number=season_numbers.get("assists"),
        )

    def _parse_card_description(self, text: str) -> Tuple[Optional[str], Optional[int]]:
        if not text:
            return None, None

        # Extract number and card type
        match = re.search(r"(\d+)\.\s*(?:Yellow|Red|yellow|red)\s*card", text)
        season_number = int(match.group(1)) if match else None

        # Extract reason (everything after card type and comma)
        reason_match = re.search(r"(?:Yellow|Red|yellow|red)\s*card\s*,\s*(.+)", text)
        reason = reason_match.group(1).strip() if reason_match else None

        return reason, season_number

    def _extract_id_from_href(self, href: str) -> str:
        """Extract ID from href URL - WORKING logic from snippet 2"""
        if not href:
            return ""

        patterns = [
            r"/verein/(\d+)",
            r"/spieler/(\d+)",
            r"/trainer/(\d+)",
            r"/schiedsrichter/(\d+)",
            r"/wettbewerb/([A-Z0-9]+)",
            r"spielbericht/(\d+)",
        ]

        for pattern in patterns:
            match = re.search(pattern, href)
            if match:
                return match.group(1)

        return ""

    def _extract_number_from_text(self, text: str) -> Optional[int]:
        """Extract first number from text"""
        match = re.search(r"(\d+)", text)
        return int(match.group(1)) if match else None

    def _extract_shirt_number(self, text: str) -> Optional[int]:
        """Extract shirt number from text"""
        if text.isdigit():
            return int(text)
        return None

    def _extract_league_position(self, text: str) -> Optional[int]:
        """Extract league position from position text"""
        match = re.search(r"Position:\s*(\d+)", text)
        return int(match.group(1)) if match else None

    def _parse_date(self, date_str: str) -> Optional[str]:
        """Parse date string to ISO format"""
        if not date_str:
            return None

        # Handle different date formats
        date_patterns = [
            (r"(\w+), (\d{1,2})/(\d{1,2})/(\d{2})", self._parse_us_date),
            (r"(\d{1,2})\.(\d{1,2})\.(\d{4})", self._parse_eu_date),
            (r"(\d{4})-(\d{1,2})-(\d{1,2})", self._parse_iso_date),
        ]

        for pattern, parser in date_patterns:
            match = re.search(pattern, date_str)
            if match:
                return parser(match.groups())

        return None

    def _parse_us_date(self, groups: tuple) -> str:
        """Parse US format date"""
        day_name, month, day, year = groups
        year = f"20{year}" if len(year) == 2 else year
        return f"{year}-{month.zfill(2)}-{day.zfill(2)}"

    def _parse_eu_date(self, groups: tuple) -> str:
        """Parse European format date"""
        day, month, year = groups
        return f"{year}-{month.zfill(2)}-{day.zfill(2)}"

    def _parse_iso_date(self, groups: tuple) -> str:
        """Parse ISO format date"""
        year, month, day = groups
        return f"{year}-{month.zfill(2)}-{day.zfill(2)}"

    def _parse_score_from_text(self, text: str) -> Optional[Tuple[int, int]]:
        """Parse score from text like '2:1' or '0:1'"""
        match = re.search(r"(\d+):(\d+)", text)
        if match:
            return (int(match.group(1)), int(match.group(2)))
        return None

    def _extract_player_portrait_url(self, href: str) -> Optional[str]:
        """Generate player portrait URL from profile href"""
        player_id = self._extract_id_from_href(href)
        if player_id:
            return f"https://img.a.transfermarkt.technology/portrait/small/{player_id}-{int(datetime.now().timestamp())}.jpg?lm=1"
        return None
