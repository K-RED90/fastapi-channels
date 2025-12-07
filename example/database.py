"""
In-memory SQLite database for storing chat conversations
"""

import sqlite3
import threading
from typing import Any


class ChatDatabase:
    """Thread-safe in-memory SQLite database for chat messages"""

    def __init__(self):
        # Use :memory: for in-memory database
        self.conn = sqlite3.connect(":memory:", check_same_thread=False)
        self.conn.row_factory = sqlite3.Row  # Enable column access by name
        self.lock = threading.Lock()
        self._init_db()

    def _init_db(self):
        """Initialize database schema"""
        cursor = self.conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                room TEXT NOT NULL,
                user_id TEXT NOT NULL,
                username TEXT NOT NULL,
                text TEXT NOT NULL,
                timestamp REAL NOT NULL,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_room_timestamp 
            ON messages(room, timestamp)
        """)
        self.conn.commit()

    def save_message(
        self, room: str, user_id: str, username: str, text: str, timestamp: float
    ) -> int | None:
        """Save a message to the database"""
        with self.lock:
            cursor = self.conn.cursor()
            cursor.execute(
                """
                INSERT INTO messages (room, user_id, username, text, timestamp)
                VALUES (?, ?, ?, ?, ?)
            """,
                (room, user_id, username, text, timestamp),
            )
            self.conn.commit()
            rowid = cursor.lastrowid
            return rowid if rowid is not None else None

    def get_messages(self, room: str, limit: int | None = None) -> list[dict[str, Any]]:
        """Get messages for a room, ordered by timestamp"""
        with self.lock:
            cursor = self.conn.cursor()
            if limit:
                cursor.execute(
                    """
                    SELECT room, user_id, username, text, timestamp
                    FROM messages
                    WHERE room = ?
                    ORDER BY timestamp ASC
                    LIMIT ?
                """,
                    (room, limit),
                )
            else:
                cursor.execute(
                    """
                    SELECT room, user_id, username, text, timestamp
                    FROM messages
                    WHERE room = ?
                    ORDER BY timestamp ASC
                """,
                    (room,),
                )

            rows = cursor.fetchall()
            messages = []
            for row in rows:
                messages.append(
                    {
                        "type": "chat_message",
                        "room": row["room"],
                        "user_id": row["user_id"],
                        "username": row["username"],
                        "text": row["text"],
                        "timestamp": row["timestamp"],
                    }
                )
            return messages

    def get_recent_messages(self, room: str, limit: int = 50) -> list[dict[str, Any]]:
        """Get recent messages for a room, ordered by timestamp (most recent last)"""
        with self.lock:
            cursor = self.conn.cursor()
            cursor.execute(
                """
                SELECT room, user_id, username, text, timestamp
                FROM messages
                WHERE room = ?
                ORDER BY timestamp DESC
                LIMIT ?
            """,
                (room, limit),
            )

            rows = cursor.fetchall()
            messages = []
            for row in rows:
                messages.append(
                    {
                        "type": "chat_message",
                        "room": row["room"],
                        "user_id": row["user_id"],
                        "username": row["username"],
                        "text": row["text"],
                        "timestamp": row["timestamp"],
                    }
                )
            # Reverse to get chronological order (oldest first)
            messages.reverse()
            return messages

    def close(self):
        """Close the database connection"""
        with self.lock:
            self.conn.close()
