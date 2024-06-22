"""
Grand Comics Database™ (https://www.comics.org) information source
"""

# Copyright comictagger team
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
from __future__ import annotations

import argparse
import json
import logging
import pathlib
import re
import sqlite3
from typing import Any, Callable, TypedDict
from urllib.parse import urljoin

import requests
import settngs
from bs4 import BeautifulSoup
from comicapi import utils
from comicapi.genericmetadata import ComicSeries, GenericMetadata, MetadataOrigin
from comicapi.issuestring import IssueString
from comictalker.comiccacher import ComicCacher
from comictalker.comiccacher import Issue as CCIssue
from comictalker.comiccacher import Series as CCSeries
from comictalker.comictalker import ComicTalker, TalkerDataError, TalkerNetworkError
from pyrate_limiter import Limiter, RequestRate
from urllib3.exceptions import LocationParseError
from urllib3.util import parse_url

logger = logging.getLogger(f"comictalker.{__name__}")


class GCDSeries(TypedDict, total=False):
    count_of_issues: int | None
    notes: str
    id: int
    name: str
    sort_name: str | None
    publisher_name: str | None
    format: str
    year_began: int | None
    year_ended: int | None
    image: str | None
    cover_downloaded: bool


class GCDIssue(TypedDict, total=False):
    id: int
    key_date: str
    number: str
    issue_title: str
    series_id: int
    issue_notes: str
    volume: int
    imprint: str
    price: str
    isbn: str
    maturity_rating: str
    country: str
    country_iso: str
    story_ids: list[str]  # CSV int - Used to gather credits from gcd_story_credit
    characters: list[str]
    language: str
    language_iso: str
    story_titles: list[str]  # combined gcd_story title_inferred and type_id for display title
    genres: list[str]  # gcd_story semicolon separated
    synopses: list[str]  # combined gcd_story synopsis
    image: str
    alt_image_urls: list[str]  # generated via variant_of_id
    credits: list[
        GCDCredit
    ]  # gcd_issue_credit and gcd_story_credit (using story_id) and gcd_credit_type and gcd_creator
    covers_downloaded: bool


limiter = Limiter(RequestRate(10, 10))


class GCDCredit(TypedDict):
    name: str
    gcd_role: str


class GCDTalker(ComicTalker):
    name: str = "Grand Comics Database"
    id: str = "gcd"
    comictagger_min_ver: str = "1.6.0a13"
    website: str = "https://www.comics.org/"
    logo_url: str = "https://files1.comics.org/static/img/gcd_logo.aaf0e64616e2.png"
    attribution: str = (
        f"Data from <a href='{website}'>{name}</a> (<a href='http://creativecommons.org/licenses/by/3.0/'>"
        f"CCA license</a>)"
    )
    about: str = (
        f"<a href='{website}'>{name}™</a> is an ongoing international project to build a detailed "
        f"comic-book database that will be easy to use and understand, and also easy for contributors to "
        f"add information to it."
    )

    def __init__(self, version: str, cache_folder: pathlib.Path):
        super().__init__(version, cache_folder)
        # Default settings
        self.db_file: pathlib.Path = pathlib.Path.home()
        self.use_series_start_as_volume: bool = False
        self.prefer_story_titles: bool = False
        self.combine_notes: bool = False
        self.use_ongoing_issue_count: bool = False
        self.currency: str = ""
        self.download_gui_covers: bool = False
        self.download_tag_covers: bool = False

        self.has_issue_id_type_id_index: bool = False
        self.has_fts5: bool = False
        self.has_fts5_checked: bool = False

        self.nn_is_issue_one: bool = True
        self.replace_nn_with_one: bool = False

    def register_settings(self, parser: settngs.Manager) -> None:
        parser.add_setting(
            "--gcd-use-series-start-as-volume",
            default=False,
            action=argparse.BooleanOptionalAction,
            display_name="Use series start as volume",
            help="Use the series start year as the volume number",
        )
        parser.add_setting(
            "--gcd-use-ongoing",
            default=False,
            action=argparse.BooleanOptionalAction,
            display_name="Use the ongoing issue count",
            help='If a series is labelled as "ongoing", use the current issue count (otherwise empty)',
        )
        parser.add_setting(
            "--gcd-nn-is-issue-one",
            default=False,
            action=argparse.BooleanOptionalAction,
            display_name="Auto-tag: Issue number of 'nn' are considered as 1 (TPB etc.)",
            help="Single issues such as TPB are given the issue number 'nn', consider those as issue 1 when using auto-tag",
        )
        parser.add_setting(
            "--gcd-replace-nn-with-one",
            default=False,
            action=argparse.BooleanOptionalAction,
            display_name="Replace issue number '[nn]' with '1' (only on final issue tagging)",
            help="Replaces the issue number '[nn]' with a '1'. (Will not show in issue window etc.)",
        )
        parser.add_setting(
            "--gcd-prefer-story-titles",
            default=False,
            action=argparse.BooleanOptionalAction,
            display_name="Prefer the story title(s) over issue title",
            help="Use the story title(s) even if there is an issue title",
        )
        parser.add_setting(
            "--gcd-combine-notes",
            default=False,
            action=argparse.BooleanOptionalAction,
            display_name="Combine series/issue notes along with synopses",
            help="Prepend any series or issue notes along with any synopses",
        )
        parser.add_setting(
            "--gcd-gui-covers",
            default=False,
            action=argparse.BooleanOptionalAction,
            display_name="Attempt to download covers for the GUI",
            help="Attempt to download covers for use in series and issue list windows",
        )
        parser.add_setting(
            "--gcd-tag-covers",
            default=False,
            action=argparse.BooleanOptionalAction,
            display_name="Attempt to download covers for auto-tagging",
            help="Attempt to download covers for use with auto-tagging",
        )
        parser.add_setting(
            "--gcd-currency",
            default="USD",
            display_name="Preferred currency",
            help="Preferred currency for price: USD, EUR, GBP, etc. (default: USD)",
        )
        parser.add_setting(
            f"--{self.id}-url",
            file=True,
            cmdline=False,
            default="Leave empty, for testing DB only",
            display_name="DB Test",
            help="For DB testing only",
        )
        parser.add_setting(f"--{self.id}-key", file=False, cmdline=False)
        parser.add_setting(
            "--gcd-filepath",
            display_name="SQLite GCD DB",
            type=pathlib.Path,
            default=pathlib.Path.home(),
            help="The path and filename of the GCD SQLite file",
        )

    def parse_settings(self, settings: dict[str, Any]) -> dict[str, Any]:
        settings = super().parse_settings(settings)

        self.use_series_start_as_volume = settings["gcd_use_series_start_as_volume"]
        self.nn_is_issue_one = settings["gcd_nn_is_issue_one"]
        self.replace_nn_with_one = settings["gcd_replace_nn_with_one"]
        self.prefer_story_titles = settings["gcd_prefer_story_titles"]
        self.combine_notes = settings["gcd_combine_notes"]
        self.use_ongoing_issue_count = settings["gcd_use_ongoing"]
        self.currency = settings["gcd_currency"]
        self.download_gui_covers = settings["gcd_gui_covers"]
        self.download_tag_covers = settings["gcd_tag_covers"]
        self.db_file = settings["gcd_filepath"]
        return settings

    def check_status(self, settings: dict[str, Any]) -> tuple[str, bool]:
        # Check file exists
        if pathlib.Path(settings["gcd_filepath"]).is_file():
            try:
                with sqlite3.connect(settings["gcd_filepath"]) as con:
                    con.row_factory = sqlite3.Row
                    con.text_factory = str
                    cur = con.cursor()
                    cur.execute("SELECT * FROM gcd_credit_type")

                    cur.fetchone()

                return "The DB access test was successful", True

            except sqlite3.Error:
                return "DB access failed", False
        else:
            return "DB path does not exist", False

    def check_create_index(self) -> None:
        self.check_db_filename_not_empty()

        # Without this index the current issue list query is VERY slow
        if not self.has_issue_id_type_id_index:
            try:
                with sqlite3.connect(self.db_file) as con:
                    con.row_factory = sqlite3.Row
                    con.text_factory = str
                    cur = con.cursor()

                    cur.execute("SELECT name FROM sqlite_master WHERE type = 'index' AND name = 'issue_id_on_type_id';")

                    if cur.fetchone():
                        self.has_issue_id_type_id_index = True
                    else:
                        # Create the index
                        cur.execute("CREATE INDEX issue_id_on_type_id ON gcd_story (type_id, issue_id);")
                        self.has_issue_id_type_id_index = True

            except sqlite3.DataError as e:
                logger.debug(f"DB data error: {e}")
                raise TalkerDataError(self.name, 1, str(e))
            except sqlite3.Error as e:
                logger.debug(f"DB error: {e}")
                raise TalkerDataError(self.name, 0, str(e))

    def check_db_filename_not_empty(self) -> None:
        if not self.db_file:
            raise TalkerDataError(self.name, 3, "Database path is empty, specify a path and filename!")
        if not pathlib.Path(self.db_file).is_file():
            raise TalkerDataError(self.name, 3, "Database path or filename is invalid!")

    def check_db_fts5(self) -> None:
        try:
            with sqlite3.connect(self.db_file) as con:
                con = sqlite3.connect(":memory:")
                cur = con.cursor()
                cur.execute("pragma compile_options;")

                if ("ENABLE_FTS5",) not in cur.fetchall():
                    logger.debug("SQLite has no FTS5 support!")
                    self.has_fts5_checked = True
                    return

        except sqlite3.Error as e:
            logger.debug(f"DB error: {e}")
            raise TalkerDataError(self.name, 0, str(e))

        try:
            with sqlite3.connect(self.db_file) as con:
                con.row_factory = sqlite3.Row
                con.text_factory = str
                cur = con.cursor()
                cur.execute("SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'fts';")

                if cur.fetchone():
                    self.has_fts5 = True
                    self.has_fts5_checked = True
                    return
                else:
                    # Create the FTS5 table
                    cur.execute(
                        "CREATE VIRTUAL TABLE fts USING fts5(name, content='gcd_series', content_rowid='id', "
                        "tokenize = 'porter unicode61 remove_diacritics 1');"
                    )
                    cur.execute("INSERT INTO fts(fts) VALUES('rebuild');")

        except sqlite3.DataError as e:
            logger.debug(f"DB data error: {e}")
            raise TalkerDataError(self.name, 1, str(e))
        except sqlite3.Error as e:
            logger.debug(f"DB error: {e}")
            raise TalkerDataError(self.name, 0, str(e))

    def search_for_series(
        self,
        series_name: str,
        callback: Callable[[int, int], None] | None = None,
        refresh_cache: bool = False,
        literal: bool = False,
        series_match_thresh: int = 90,
    ) -> list[ComicSeries]:
        sql_search: str = ""
        sql_search_fields: str = """SELECT gcd_series.id AS 'id', gcd_series.name AS 'series_name',
                            gcd_series.sort_name AS 'sort_name', gcd_series.notes AS 'notes',
                            gcd_series.year_began AS 'year_began', gcd_series.year_ended AS 'year_ended',
                            gcd_series.issue_count AS 'issue_count', gcd_publisher.name AS 'publisher_name' """

        sql_literal_search: str = """FROM gcd_publisher
                    LEFT JOIN gcd_series ON gcd_series.publisher_id=gcd_publisher.id
                    WHERE gcd_series.name = ?"""

        sql_like_search: str = """FROM gcd_publisher
                    LEFT JOIN gcd_series ON gcd_series.publisher_id=gcd_publisher.id
                    WHERE gcd_series.name LIKE ?"""

        sql_ft_search: str = """FROM fts
                    LEFT JOIN gcd_series on fts.rowid=gcd_series.id
                    LEFT JOIN gcd_publisher ON gcd_series.publisher_id=gcd_publisher.id
                    WHERE fts MATCH ?;"""

        self.check_db_filename_not_empty()
        if not self.has_fts5_checked:
            self.check_db_fts5()

        search_series_name = series_name
        if literal:
            # This will be literally literal: "the" will not match "The" etc.
            sql_search = sql_search_fields + sql_literal_search
        elif not self.has_fts5:
            # Make the search fuzzier
            search_series_name = search_series_name.replace(" ", "%") + "%"
            sql_search = sql_search_fields + sql_like_search
        else:
            # Order is important
            # Escape any single and double quotes
            search_series_name = search_series_name.replace("'", "''")
            search_series_name = search_series_name.replace('"', '""')
            # Now format for full-text search by tokenizing each word with surrounding double quotes
            search_series_name = '"' + search_series_name + '"'
            search_series_name = search_series_name.replace(" ", '" "')

            # Use FTS5 for search
            sql_search = sql_search_fields + sql_ft_search

        results = []

        logger.info(f"{self.name} searching: {search_series_name}")

        try:
            with sqlite3.connect(self.db_file) as con:
                con.row_factory = sqlite3.Row
                con.text_factory = str
                cur = con.cursor()
                cur.execute(
                    sql_search,
                    [search_series_name],
                )
                rows = cur.fetchall()

                for record in rows:
                    result = GCDSeries(
                        id=record["id"],
                        name=record["series_name"],
                        sort_name=record["sort_name"],
                        notes=record["notes"],
                        year_began=record["year_began"],
                        year_ended=record["year_ended"],
                        count_of_issues=record["issue_count"],
                        publisher_name=record["publisher_name"],
                        format="",
                        image="",
                        cover_downloaded=False,
                    )

                    results.append(result)

        except sqlite3.DataError as e:
            logger.debug(f"DB data error: {e}")
            raise TalkerDataError(self.name, 1, str(e))
        except sqlite3.Error as e:
            logger.debug(f"DB error: {e}")
            raise TalkerDataError(self.name, 0, str(e))

        formatted_search_results = self._format_search_results(results)

        return formatted_search_results

    def fetch_comic_data(
        self, issue_id: str | None = None, series_id: str | None = None, issue_number: str = ""
    ) -> GenericMetadata:
        self.check_db_filename_not_empty()

        comic_data = GenericMetadata()
        if issue_id:
            comic_data = self._fetch_issue_data_by_issue_id(int(issue_id))
        elif issue_number and series_id:
            comic_data = self._fetch_issue_data(int(series_id), issue_number)

        return comic_data

    def fetch_issues_in_series(self, series_id: str) -> list[GenericMetadata]:
        series = self._fetch_series_data(int(series_id))

        results: list[GCDIssue] = []

        self.check_create_index()

        try:
            with sqlite3.connect(self.db_file) as con:
                con.row_factory = sqlite3.Row
                con.text_factory = str
                cur = con.cursor()
                cur.execute(
                    "SELECT gcd_issue.id AS 'id', gcd_issue.number AS 'number', gcd_issue.key_date AS 'key_date',"
                    " gcd_issue.title AS 'issue_title', gcd_issue.series_id AS 'series_id', "
                    "GROUP_CONCAT(CASE WHEN gcd_story.title IS NOT NULL AND gcd_story.title != '' THEN gcd_story.title "
                    "ELSE NULL END, '\n') AS 'story_titles' "
                    "FROM gcd_issue "
                    "LEFT JOIN gcd_story ON gcd_story.issue_id = gcd_issue.id AND gcd_story.type_id = 19 "
                    "WHERE gcd_issue.series_id = ? "
                    "GROUP BY gcd_issue.number;",
                    [int(series_id)],
                )
                rows = cur.fetchall()

                if rows:
                    for record in rows:
                        results.append(self._format_gcd_issue(record))

                # No issue(s) found
                else:
                    return [GenericMetadata()]

        except sqlite3.DataError as e:
            logger.debug(f"DB data error: {e}")
            raise TalkerDataError(self.name, 1, str(e))
        except sqlite3.Error as e:
            logger.debug(f"DB error: {e}")
            raise TalkerDataError(self.name, 0, str(e))

        formatted_series_issues_result = [self._map_comic_issue_to_metadata(x, series) for x in results]

        return formatted_series_issues_result

    def fetch_issues_by_series_issue_num_and_year(
        self, series_id_list: list[str], issue_number: str, year: int | None
    ) -> list[GenericMetadata]:
        results: list[GenericMetadata] = []
        year_search = "%"
        if year:
            year_search = str(year) + "%"

        self.check_create_index()

        sql_search: str = ""

        sql_search_main: str = """SELECT gcd_issue.id AS 'id', gcd_issue.key_date AS 'key_date', gcd_issue.number AS
                        'number', gcd_issue.title AS 'issue_title', gcd_issue.series_id AS 'series_id',
                        GROUP_CONCAT(CASE WHEN gcd_story.title IS NOT NULL AND gcd_story.title != '' THEN
                        gcd_story.title END, '\n') AS 'story_titles'
                        FROM gcd_issue
                        LEFT JOIN gcd_story ON gcd_story.issue_id=gcd_issue.id AND gcd_story.type_id=19
                        WHERE gcd_issue.series_id=? """

        sql_search_issues: str = "AND gcd_issue.number=? AND (gcd_issue.key_date LIKE ? OR gcd_issue.key_date='') "

        sql_search_issues_nn: str = """AND (gcd_issue.number=? OR gcd_issue.number='[nn]') AND
                        (gcd_issue.key_date LIKE ? OR gcd_issue.key_date='') """

        sql_search_group: str = "GROUP BY gcd_issue.number;"

        if self.nn_is_issue_one and issue_number == "1":
            sql_search = sql_search_main + sql_search_issues_nn + sql_search_group
        else:
            sql_search = sql_search_main + sql_search_issues + sql_search_group

        try:
            with sqlite3.connect(self.db_file) as con:
                con.row_factory = sqlite3.Row
                con.text_factory = str
                cur = con.cursor()

                for vid in series_id_list:
                    series = self._fetch_series_data(int(vid))

                    cur.execute(
                        sql_search,
                        [vid, issue_number, year_search],
                    )

                    rows = cur.fetchall()

                    if rows:
                        for record in rows:
                            issue = self._format_gcd_issue(record)

                            # Download covers for matching
                            if self.download_tag_covers:
                                image, variants = self._find_issue_images(issue["id"])
                                issue["image"] = image
                                issue["alt_image_urls"] = variants
                                issue["covers_downloaded"] = True

                            results.append(self._map_comic_issue_to_metadata(issue, series))

        except sqlite3.DataError as e:
            logger.debug(f"DB data error: {e}")
            raise TalkerDataError(self.name, 1, str(e))
        except sqlite3.Error as e:
            logger.debug(f"DB error: {e}")
            raise TalkerDataError(self.name, 0, str(e))

        return results

    def _match_format(self, string: str) -> str | None:
        # The publishing_format field is a free-text mess, try and make something useful
        word_list = [
            "annual",
            "album",
            "anthology",
            "collection",
            r"collect.*",
            "graphic novel",
            "hardcover",
            "limited series",
            r"one[-\s]?shot",
            "preview",
            "special",
            r"trade paper[\s]?back",
            r"web[\s]?comic",
            r"mini[-\s]?series",
        ]

        pattern = r"\b(?:" + "|".join(word_list) + r")\b"
        match = re.search(pattern, string, re.IGNORECASE)

        if match:
            if "collect" in match.group(0).casefold():
                return "Collection"
            return match.group(0).title()
        else:
            return None

    def _find_series_image(self, series_id: int) -> str:
        """Find the id of the first issue and get the image url"""
        issue_id = None
        cover = ""
        try:
            with sqlite3.connect(self.db_file) as con:
                con.row_factory = sqlite3.Row
                con.text_factory = str
                cur = con.cursor()

                cur.execute(
                    "SELECT gcd_series.first_issue_id " "FROM gcd_series " "WHERE gcd_series.id=?",
                    [series_id],
                )
                issue_id = cur.fetchone()[0]

        except sqlite3.DataError as e:
            logger.debug(f"DB data error: {e}")
            raise TalkerDataError(self.name, 1, str(e))
        except sqlite3.Error as e:
            logger.debug(f"DB error: {e}")
            raise TalkerDataError(self.name, 0, str(e))

        if issue_id:
            cover, _ = self._find_issue_images(issue_id)

        return cover

    def _find_issue_images(self, issue_id: int) -> tuple[str, list[str]]:
        """Fetch images for the issue id"""
        cover = ""
        variants = []

        with limiter.ratelimit("default", delay=True):
            try:
                covers_html = requests.get(f"{self.website}/issue/{issue_id}/cover/4").text
            except requests.exceptions.Timeout:
                logger.debug(f"Connection to {self.website} timed out.")
                raise TalkerNetworkError(self.website, 4)
            except requests.exceptions.RequestException as e:
                logger.debug(f"Request exception: {e}")
                raise TalkerNetworkError(self.website, 0, str(e)) from e

        covers_page = BeautifulSoup(covers_html, "html.parser")

        img_list = covers_page.findAll("img", "cover_img")

        if len(img_list) > 0:
            for i, image in enumerate(img_list):
                # Strip arbitrary number from end for cache
                src = image.get("src").split("?")[0]
                if i == 0:
                    cover = src
                else:
                    variants.append(src)
        else:
            cf_challenge = covers_page.findAll(id="challenge-error-title")
            if cf_challenge:
                logger.info(f"CloudFlare active, cannot access image for ID: {issue_id}")
            else:
                logger.info(f"No image found for ID: {issue_id}")

        return cover, variants

    def _find_issue_credits(self, issue_id: int, story_id_list: list[str]) -> list[GCDCredit]:
        credit_results = []
        # First get the issue table credits
        try:
            with sqlite3.connect(self.db_file) as con:
                con.row_factory = sqlite3.Row
                con.text_factory = str
                cur = con.cursor()
                cur.execute(
                    "SELECT gcd_issue_credit.credit_name AS 'role', gcd_creator_name_detail.name "
                    "FROM gcd_issue_credit "
                    "INNER JOIN gcd_creator_name_detail ON gcd_issue_credit.creator_id=gcd_creator_name_detail.id "
                    "WHERE gcd_issue_credit.issue_id=?",
                    [issue_id],
                )
                rows = cur.fetchall()

                for record in rows:
                    result = GCDCredit(
                        name=record[1],
                        gcd_role=record[0],
                    )

                    credit_results.append(result)

                # Get story table credits
                for story_id in story_id_list:
                    cur.execute(
                        "SELECT gcd_creator_name_detail.name, gcd_credit_type.name "
                        "FROM gcd_story_credit "
                        "INNER JOIN gcd_credit_type ON gcd_credit_type.id=gcd_story_credit.credit_type_id "
                        "INNER JOIN gcd_creator_name_detail ON gcd_creator_name_detail.id=gcd_story_credit.creator_id "
                        "WHERE gcd_story_credit.story_id=?",
                        [int(story_id)],
                    )
                    rows = cur.fetchall()

                    for record in rows:
                        result = GCDCredit(
                            name=record[0],
                            gcd_role=record[1],
                        )

                        credit_results.append(result)

        except sqlite3.DataError as e:
            logger.debug(f"DB data error: {e}")
            raise TalkerDataError(self.name, 1, str(e))
        except sqlite3.Error as e:
            logger.debug(f"DB error: {e}")
            raise TalkerDataError(self.name, 0, str(e))

        return credit_results

    # Search results and full series data
    def _format_search_results(self, search_results: list[GCDSeries]) -> list[ComicSeries]:
        formatted_results = []
        for record in search_results:
            formatted_results.append(
                ComicSeries(
                    aliases=set(),
                    count_of_issues=record.get("count_of_issues"),
                    count_of_volumes=None,
                    description=record.get("notes"),
                    id=str(record["id"]),
                    image_url=record.get("image", ""),
                    name=record["name"],
                    publisher=record["publisher_name"],
                    format=None,
                    start_year=record["year_began"],
                )
            )

        return formatted_results

    def _format_gcd_issue(self, row: sqlite3.Row, complete: bool = False) -> GCDIssue:
        # Convert for attribute access
        row_dict = dict(row)

        gcd_issue = GCDIssue(
            id=row_dict["id"],
            key_date=row_dict["key_date"],
            number=row_dict["number"],
            issue_title=row_dict["issue_title"],
            series_id=row_dict["series_id"],
            story_titles=(
                row_dict["story_titles"].split("\n")
                if "story_titles" in row_dict and row_dict["story_titles"] is not None
                else []
            ),
            synopses=(
                row_dict["synopses"].split("\n\n")
                if "synopses" in row_dict and row_dict["synopses"] is not None
                else []
            ),
            image="",
            alt_image_urls=[],
            covers_downloaded=False,
        )

        if complete:
            gcd_issue["issue_notes"] = row_dict["issue_notes"]
            gcd_issue["volume"] = row_dict["volume"]
            gcd_issue["price"] = row_dict["price"]
            gcd_issue["isbn"] = row_dict["isbn"]
            gcd_issue["imprint"] = row_dict["imprint"]
            gcd_issue["maturity_rating"] = row_dict["maturity_rating"]
            gcd_issue["characters"] = (
                row_dict["characters"].split("; ") if "characters" in row_dict and row_dict["characters"] else []
            )
            gcd_issue["country"] = row_dict["country"]
            gcd_issue["country_iso"] = row_dict["country_iso"]
            gcd_issue["story_ids"] = (
                row_dict["story_ids"].split("\n") if "story_ids" in row_dict and row_dict["story_ids"] else []
            )
            gcd_issue["language"] = row_dict["language"]
            gcd_issue["language_iso"] = row_dict["language_iso"]
            gcd_issue["genres"] = (
                [genre.strip().capitalize() for genre in row_dict.get("genres", "").split(";")]
                if "genres" in row_dict and row_dict["genres"]
                else []
            )
            gcd_issue["credits"] = []

        return gcd_issue

    def fetch_series(self, series_id: str) -> ComicSeries:
        return self._format_search_results([self._fetch_series_data(int(series_id))])[0]

    def _fetch_series_data(self, series_id: int) -> GCDSeries:
        cvc = ComicCacher(self.cache_folder, self.version)
        cached_series = cvc.get_series_info(str(series_id), self.id)

        if cached_series is not None and cached_series[1]:
            cache = json.loads(cached_series[0].data)
            # Even though the cache is "complete", downloading the cover is an option
            if self.download_gui_covers and cache["cover_downloaded"]:
                return cache
            elif not self.download_gui_covers:
                return cache
            # While an else could go here to fetch the cover, might as well refresh all the data

        try:
            with sqlite3.connect(self.db_file) as con:
                con.row_factory = sqlite3.Row
                con.text_factory = str
                cur = con.cursor()

                cur.execute(
                    "SELECT gcd_series.id AS 'id', gcd_series.name AS 'series_name', "
                    "gcd_series.sort_name AS 'sort_name', gcd_series.notes AS 'notes', "
                    "gcd_series.year_began AS 'year_began', gcd_series.year_ended AS 'year_ended', "
                    "gcd_series.issue_count AS 'issue_count', gcd_publisher.name AS 'publisher_name', "
                    "gcd_series.country_id AS 'country_id', gcd_series.language_id AS 'lang_id', "
                    "gcd_series.publishing_format AS 'format', gcd_series.is_current AS 'is_current' "
                    "FROM gcd_publisher "
                    "LEFT JOIN gcd_series ON gcd_series.publisher_id=gcd_publisher.id "
                    "WHERE gcd_series.id=?",
                    [series_id],
                )
                row = cur.fetchone()

                # Scrape GCD for series cover URL
                image = ""
                cover_download = False
                if self.download_gui_covers:
                    image = self._find_series_image(series_id)
                    cover_download = True

                result = GCDSeries(
                    id=row["id"],
                    name=row["series_name"],
                    sort_name=row["sort_name"],
                    notes=row["notes"],
                    year_began=row["year_began"],
                    year_ended=row["year_ended"],
                    count_of_issues=row["issue_count"],
                    publisher_name=row["publisher_name"],
                    format=row["format"],
                    image=image,
                    cover_downloaded=cover_download,
                )

        except sqlite3.DataError as e:
            logger.debug(f"DB data error: {e}")
            raise TalkerDataError(self.name, 1, str(e))
        except sqlite3.Error as e:
            logger.debug(f"DB error: {e}")
            raise TalkerDataError(self.name, 0, str(e))

        if result:
            cvc.add_series_info(self.id, CCSeries(id=str(result["id"]), data=json.dumps(result).encode("utf-8")), True)

        return result

    def _fetch_issue_data(self, series_id: int, issue_number: str) -> GenericMetadata:
        # Find the id of the issue and pass it along

        sql_query: str = ""
        sql_base: str = """SELECT gcd_issue.id AS 'id'
                    FROM gcd_issue """
        sql_where: str = "WHERE gcd_issue.series_id=? AND gcd_issue.number=?"
        sql_where_nn: str = "WHERE gcd_issue.series_id=? AND gcd_issue.number=? OR gcd_issue.number='[nn]'"

        if self.nn_is_issue_one and issue_number == "1":
            sql_query = sql_base + sql_where_nn
        else:
            sql_query = sql_base + sql_where

        try:
            with sqlite3.connect(self.db_file) as con:
                con.row_factory = sqlite3.Row
                con.text_factory = str
                cur = con.cursor()

                cur.execute(
                    sql_query,
                    [series_id, issue_number],
                )
                row = cur.fetchone()

                if row["id"]:
                    return self._fetch_issue_data_by_issue_id(row["id"])

        except sqlite3.DataError as e:
            logger.debug(f"DB data error: {e}")
            raise TalkerDataError(self.name, 1, str(e))
        except sqlite3.Error as e:
            logger.debug(f"DB error: {e}")
            raise TalkerDataError(self.name, 0, str(e))

        return GenericMetadata()

    def _fetch_issue_data_by_issue_id(self, issue_id: int) -> GenericMetadata:
        issue = self._fetch_issue_by_issue_id(issue_id)
        series = self._fetch_series_data(issue["series_id"])

        return self._map_comic_issue_to_metadata(issue, series)

    def _fetch_issue_by_issue_id(self, issue_id: int) -> GCDIssue:
        cvc = ComicCacher(self.cache_folder, self.version)
        cached_issue = cvc.get_issue_info(str(issue_id), self.id)

        if cached_issue and cached_issue[1]:
            cache = json.loads(cached_issue[0].data)
            # Even though the cache is "complete", downloading the cover is an option
            if self.download_gui_covers and cache["covers_downloaded"]:
                return cache
            elif not self.download_gui_covers:
                return cache
            # While an else could go here to fetch the cover, might as well refresh all the data

        # Need this one?
        self.check_create_index()

        try:
            with sqlite3.connect(self.db_file) as con:
                con.row_factory = sqlite3.Row
                con.text_factory = str
                cur = con.cursor()

                cur.execute(
                    "SELECT gcd_issue.id AS 'id', gcd_issue.key_date AS 'key_date', gcd_issue.number AS 'number', "
                    "gcd_issue.title AS 'issue_title', gcd_issue.series_id AS 'series_id', "
                    "gcd_issue.price AS 'price', gcd_issue.valid_isbn AS 'isbn', "
                    "gcd_issue.notes AS 'issue_notes', gcd_issue.volume AS 'volume', "
                    "gcd_issue.rating AS 'maturity_rating', gcd_story.characters AS 'characters', "
                    "stddata_country.name AS 'country', stddata_country.code AS 'country_iso', "
                    "stddata_language.name AS 'language', stddata_language.code AS 'language_iso', "
                    "GROUP_CONCAT(CASE WHEN gcd_story.title IS NOT NULL AND gcd_story.title != '' THEN "
                    "gcd_story.title END, '\n') AS 'story_titles',"
                    "GROUP_CONCAT(CASE WHEN gcd_story.genre IS NOT NULL AND gcd_story.genre != '' THEN "
                    "gcd_story.genre END, ';') AS 'genres',"
                    "GROUP_CONCAT(CASE WHEN gcd_story.synopsis IS NOT NULL AND gcd_story.synopsis != '' THEN "
                    "gcd_story.synopsis END,'\n\n') AS 'synopses', "
                    "GROUP_CONCAT(CASE WHEN gcd_story.id IS NOT NULL AND gcd_story.id != '' THEN "
                    "gcd_story.id END, '\n') AS 'story_ids', "
                    "(SELECT GROUP_CONCAT(gcd_brand_group.name, '; ') "
                    "from gcd_issue "
                    "LEFT JOIN gcd_brand ON gcd_issue.brand_id=gcd_brand.id "
                    "LEFT JOIN gcd_brand_emblem_group ON gcd_brand.id=gcd_brand_emblem_group.brand_id "
                    "LEFT JOIN gcd_brand_group ON gcd_brand_emblem_group.brandgroup_id=gcd_brand_group.id "
                    "LEFT JOIN gcd_series ON gcd_issue.series_id=gcd_series.id "
                    "LEFT JOIN gcd_publisher ON gcd_series.publisher_id=gcd_publisher.id "
                    "WHERE gcd_issue.id=? "
                    "and gcd_publisher.name is not gcd_brand_group.name "
                    ") as 'imprint' "
                    "FROM gcd_issue "
                    "LEFT JOIN gcd_story ON gcd_story.issue_id=gcd_issue.id AND gcd_story.type_id=19 "
                    "LEFT JOIN gcd_indicia_publisher ON gcd_issue.indicia_publisher_id=gcd_indicia_publisher.id "
                    "LEFT JOIN gcd_series ON gcd_issue.series_id=gcd_series.id "
                    "LEFT JOIN stddata_country ON gcd_indicia_publisher.country_id=stddata_country.id "
                    "LEFT JOIN stddata_language ON gcd_series.language_id=stddata_language.id "
                    "WHERE gcd_issue.id=? "
                    "GROUP BY gcd_issue.id",
                    [issue_id, issue_id],
                )
                row = cur.fetchone()

                if row:
                    issue_result = self._format_gcd_issue(row, True)
                else:
                    logger.debug(f"Issue ID {issue_id} not found")
                    raise TalkerDataError(self.name, 3, f"Issue ID {issue_id} not found")

        except sqlite3.DataError as e:
            logger.debug(f"DB data error: {e}")
            raise TalkerDataError(self.name, 1, str(e))
        except sqlite3.Error as e:
            logger.debug(f"DB error: {e}")
            raise TalkerDataError(self.name, 0, str(e))

        # Add credits
        issue_result["credits"] = self._find_issue_credits(issue_id, issue_result["story_ids"])

        # Add covers
        if self.download_gui_covers:
            image, variants = self._find_issue_images(issue_result["id"])
            issue_result["image"] = image
            issue_result["alt_image_urls"] = variants
            issue_result["covers_downloaded"] = True
        else:
            issue_result["covers_downloaded"] = False

        cvc.add_issues_info(
            self.id,
            [
                CCIssue(
                    id=str(issue_result["id"]),
                    series_id=str(issue_result["series_id"]),
                    data=json.dumps(issue_result).encode("utf-8"),
                )
            ],
            True,
        )

        return issue_result

    def _map_comic_issue_to_metadata(self, issue: GCDIssue, series: GCDSeries) -> GenericMetadata:
        md = GenericMetadata(
            data_origin=MetadataOrigin(self.id, self.name),
            issue_id=utils.xlate(issue["id"]),
            series_id=utils.xlate(series["id"]),
            publisher=utils.xlate(series.get("publisher_name")),
            series=utils.xlate(series["name"]),
        )
        issue_number = utils.xlate(IssueString(issue.get("number")).as_string())
        if self.replace_nn_with_one and issue_number == "[nn]":
            md.issue = "1"
        else:
            md.issue = issue_number

        md._cover_image = issue.get("image")
        md._alternate_images = issue.get("alt_image_urls")

        if issue.get("characters"):
            # Logan [disambiguation: Wolverine] - (name) James Howlett
            md.characters = set(issue["characters"])

        if issue.get("credits"):
            for person in issue["credits"]:
                md.add_credit(person["name"], person["gcd_role"])

        # It's possible to have issue_title and story_titles
        md.title = issue.get("issue_title")
        if (self.prefer_story_titles or not md.title) and issue.get("story_titles"):
            md.title = "; ".join(issue["story_titles"])

        if issue.get("genres"):
            md.genres = set(issue["genres"])

        # Price mostly in format: 00.00 CUR; 00.00 CUR
        if issue.get("price"):
            prices = issue["price"].split(";")
            for price in prices:
                if price.casefold().endswith(self.currency.casefold()):
                    md.price = utils.xlate_float(price)

        if issue.get("isbn"):
            md.identifier = issue["isbn"]

        if series["year_ended"] or self.use_ongoing_issue_count:
            md.issue_count = utils.xlate_int(series["count_of_issues"])

        # Init as string for concat
        md.description = ""
        if self.combine_notes:
            md.description = series.get("notes", "")
            md.description += issue.get("issue_notes", "")
        if len(issue["synopses"]) == len(issue["story_titles"]):
            # Will presume titles go with synopsis if there are the same number
            for i, title in enumerate(issue["story_titles"]):
                if title and issue["synopses"][i]:
                    md.description += f"{title}: {issue['synopses'][i]}\r\n\r\n"
        else:
            md.description += "\r\n\r\n".join(issue["synopses"])

        url = urljoin(self.website, f"issue/{issue['id']}")
        if url:
            try:
                md.web_links = [parse_url(url)]
            except LocationParseError:
                ...

        md.volume = utils.xlate_int(issue.get("volume"))
        if self.use_series_start_as_volume:
            md.volume = series["year_began"]

        if issue.get("key_date"):
            md.day, md.month, md.year = utils.parse_date_str(issue.get("key_date"))
        elif series["year_began"]:
            md.year = utils.xlate_int(series["year_began"])

        md.language = issue.get("language_iso")
        md.country = issue.get("country")

        md.format = self._match_format(series.get("format", ""))

        md.maturity_rating = issue.get("maturity_rating")

        md.imprint = issue.get("imprint")

        return md
