#!/usr/bin/env python3
"""Extract Apple Podcasts transcripts from the local macOS cache."""

from __future__ import annotations

import argparse
import json
import re
import sqlite3
import sys
import textwrap
import xml.etree.ElementTree as ET
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterable


DEFAULT_PODCASTS_PATH = (
    "~/Library/Group Containers/243LU875E5.groups.com.apple.podcasts"
)
APPLE_EPOCH_OFFSET = 978307200


@dataclass
class TranscriptChunk:
    speaker: str | None
    text: str


@dataclass
class Episode:
    podcast_id: str
    author: str = "Unknown"
    title: str = "Unknown"
    description: str = ""
    duration: int = -1
    released_at: int = -1
    last_modified: float = -1
    transcript_path: str = ""
    chunks: list[TranscriptChunk] | None = None

    @property
    def transcript_text(self) -> str:
        return "\n\n".join(chunk.text for chunk in self.chunks or [] if chunk.text)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="View or export locally cached Apple Podcasts transcripts.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=textwrap.dedent(
            """\
            Examples:
              %(prog)s --list
              %(prog)s --search "acquired" --list
              %(prog)s --id 1234567890
              %(prog)s --export-dir transcripts
              %(prog)s ~/Desktop/podcasts-cache --json
            """
        ),
    )
    parser.add_argument(
        "path",
        nargs="?",
        default=DEFAULT_PODCASTS_PATH,
        help="Apple Podcasts group container or another folder to scan.",
    )
    parser.add_argument("--list", action="store_true", help="List matching episodes.")
    parser.add_argument("--id", help="Print the transcript for one podcast/store id.")
    parser.add_argument("--search", help="Filter by title, author, description, or text.")
    parser.add_argument(
        "--export-dir",
        type=Path,
        help="Write matching transcripts as .txt files in this directory.",
    )
    parser.add_argument("--json", action="store_true", help="Print matching episodes as JSON.")
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Limit the number of listed/exported/JSON episodes. 0 means no limit.",
    )
    parser.add_argument(
        "--no-metadata",
        action="store_true",
        help="Skip MTLibrary.sqlite lookup and use transcript filenames only.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    root = Path(args.path).expanduser()
    if not root.exists():
        print(f"Path does not exist: {root}", file=sys.stderr)
        return 1

    episodes = load_episodes(root, include_metadata=not args.no_metadata)
    episodes = filter_episodes(episodes, args.search)

    if args.id:
        episode = next((ep for ep in episodes if ep.podcast_id == args.id), None)
        if not episode:
            print(f"No transcript found for id {args.id}", file=sys.stderr)
            return 1
        print(format_transcript(episode))
        return 0

    if args.limit > 0:
        episodes = episodes[: args.limit]

    if args.export_dir:
        export_episodes(episodes, args.export_dir)

    if args.json:
        print(json.dumps([episode_to_json(ep) for ep in episodes], indent=2))
        return 0

    if args.list or not args.export_dir:
        print(format_episode_list(episodes))

    return 0


def load_episodes(root: Path, include_metadata: bool = True) -> list[Episode]:
    transcript_paths = sorted(root.rglob("*.ttml"))
    episodes = {}
    for transcript_path in transcript_paths:
        episode = parse_transcript_file(transcript_path)
        if episode:
            episodes[episode.podcast_id] = episode

    if include_metadata and episodes:
        apply_sqlite_metadata(root, episodes)

    return sorted(
        episodes.values(),
        key=lambda ep: ep.last_modified,
        reverse=True,
    )


def parse_transcript_file(path: Path) -> Episode | None:
    match = re.search(r"(\d+)", path.name)
    if not match:
        return None

    podcast_id = match.group(1)
    try:
        tree = ET.parse(path)
    except ET.ParseError as exc:
        print(f"Skipping invalid XML {path}: {exc}", file=sys.stderr)
        return None

    root = tree.getroot()
    if not any(local_name(node.tag) == "body" for node in root.iter()):
        return None

    chunks = []
    for paragraph in root.iter():
        if local_name(paragraph.tag) != "p":
            continue
        speaker = local_attr(paragraph.attrib, "agent")
        sentences = []
        for span in paragraph.iter():
            if local_name(span.tag) == "span" and local_attr(span.attrib, "unit") == "sentence":
                text = normalize_text(" ".join(span.itertext()))
                if text:
                    sentences.append(text)
        text = normalize_text(" ".join(sentences))
        if text:
            chunks.append(TranscriptChunk(speaker=speaker, text=text))

    stat = path.stat()
    return Episode(
        podcast_id=podcast_id,
        last_modified=stat.st_mtime,
        transcript_path=str(path),
        chunks=chunks,
    )


def apply_sqlite_metadata(root: Path, episodes: dict[str, Episode]) -> None:
    db_path = next(root.rglob("MTLibrary.sqlite"), None)
    if not db_path:
        return

    try:
        connection = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    except sqlite3.Error as exc:
        print(f"Could not open {db_path}: {exc}", file=sys.stderr)
        return

    try:
        columns = {
            row[1]
            for row in connection.execute("PRAGMA table_info(ZMTEPISODE)").fetchall()
        }
        select_columns = [
            "ZSTORETRACKID",
            "ZAUTHOR",
            "ZCLEANEDTITLE",
            "ZITUNESSUBTITLE",
            "ZDURATION",
        ]
        has_first_available = "ZFIRSTTIMEAVAILABLE" in columns
        if has_first_available:
            select_columns.append("ZFIRSTTIMEAVAILABLE")

        ids = list(episodes.keys())
        placeholders = ",".join("?" for _ in ids)
        query = f"""
            SELECT {", ".join(select_columns)}
            FROM ZMTEPISODE
            WHERE ZSTORETRACKID IN ({placeholders})
        """
        for row in connection.execute(query, ids):
            podcast_id = str(row[0])
            episode = episodes.get(podcast_id)
            if not episode:
                continue
            episode.author = row[1] or episode.author
            episode.title = row[2] or episode.title
            episode.description = row[3] or episode.description
            episode.duration = int(row[4]) if row[4] is not None else episode.duration
            if has_first_available and row[5] is not None:
                episode.released_at = int(row[5]) + APPLE_EPOCH_OFFSET
    except sqlite3.Error as exc:
        print(f"Could not read podcast metadata from {db_path}: {exc}", file=sys.stderr)
    finally:
        connection.close()


def filter_episodes(episodes: list[Episode], search: str | None) -> list[Episode]:
    if not search:
        return episodes
    needle = search.casefold()
    return [
        ep
        for ep in episodes
        if needle
        in "\n".join(
            [
                ep.podcast_id,
                ep.author,
                ep.title,
                ep.description,
                ep.transcript_text,
            ]
        ).casefold()
    ]


def export_episodes(episodes: Iterable[Episode], export_dir: Path) -> None:
    export_dir.mkdir(parents=True, exist_ok=True)
    for episode in episodes:
        filename = safe_filename(f"{episode.title}-{episode.podcast_id}.txt")
        (export_dir / filename).write_text(format_transcript(episode), encoding="utf-8")


def episode_to_json(episode: Episode) -> dict:
    data = asdict(episode)
    data["released_date"] = format_date(episode.released_at)
    data["duration_text"] = format_duration(episode.duration)
    data["transcript"] = episode.transcript_text
    return data


def format_episode_list(episodes: list[Episode]) -> str:
    if not episodes:
        return "No podcast transcripts found."

    lines = []
    for episode in episodes:
        lines.append(
            "\n".join(
                [
                    f"{episode.podcast_id}  {episode.title}",
                    f"  Author: {episode.author}",
                    f"  Date: {format_date(episode.released_at)}",
                    f"  Duration: {format_duration(episode.duration)}",
                    f"  Transcript: {episode.transcript_path}",
                ]
            )
        )
    return "\n\n".join(lines)


def format_transcript(episode: Episode) -> str:
    heading = [
        episode.title,
        f"Author: {episode.author}",
        f"ID: {episode.podcast_id}",
        f"Date: {format_date(episode.released_at)}",
        f"Duration: {format_duration(episode.duration)}",
        "",
    ]
    return "\n".join(heading) + episode.transcript_text


def format_duration(total_seconds: int) -> str:
    if total_seconds == -1:
        return "Unknown"
    hours = total_seconds // 3600
    minutes = (total_seconds % 3600) // 60
    if hours and minutes:
        return f"{hours} HR {minutes} MIN"
    if hours:
        return f"{hours} HR"
    return f"{minutes} MIN"


def format_date(unix_time: int) -> str:
    if unix_time == -1:
        return "Unknown"
    return datetime.fromtimestamp(unix_time).strftime("%B %-d, %Y")


def safe_filename(value: str) -> str:
    value = re.sub(r"[^\w .()-]+", "", value).strip()
    value = re.sub(r"\s+", " ", value)
    return value or "transcript.txt"


def normalize_text(value: str) -> str:
    return re.sub(r"\s+", " ", value).strip()


def local_name(tag: str) -> str:
    return tag.rsplit("}", 1)[-1]


def local_attr(attributes: dict[str, str], name: str) -> str | None:
    for key, value in attributes.items():
        if key.rsplit("}", 1)[-1] == name:
            return value
    return None


if __name__ == "__main__":
    raise SystemExit(main())
