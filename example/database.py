"""In-memory SQLite database for storing chat conversations"""

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
        # Users table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id TEXT PRIMARY KEY,
                username TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'offline',
                joined_at REAL NOT NULL,
                last_seen REAL NOT NULL
            )
        """)
        # Rooms table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS rooms (
                room_name TEXT PRIMARY KEY,
                created_at REAL NOT NULL,
                created_by TEXT NOT NULL,
                creator_username TEXT NOT NULL,
                is_public INTEGER NOT NULL DEFAULT 1,
                user_count INTEGER NOT NULL DEFAULT 0
            )
        """)
        # User-Rooms junction table (many-to-many relationship)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS user_rooms (
                user_id TEXT NOT NULL,
                room_name TEXT NOT NULL,
                PRIMARY KEY (user_id, room_name),
                FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE,
                FOREIGN KEY (room_name) REFERENCES rooms(room_name) ON DELETE CASCADE
            )
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_user_rooms_user 
            ON user_rooms(user_id)
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_user_rooms_room 
            ON user_rooms(room_name)
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

    # User methods

    def get_user(self, user_id: str) -> dict[str, Any] | None:
        """Get user by user_id"""
        with self.lock:
            cursor = self.conn.cursor()
            cursor.execute(
                """
                SELECT user_id, username, status, joined_at, last_seen
                FROM users
                WHERE user_id = ?
            """,
                (user_id,),
            )
            row = cursor.fetchone()
            if row is None:
                return None
            # Get user's rooms
            cursor.execute(
                """
                SELECT room_name
                FROM user_rooms
                WHERE user_id = ?
            """,
                (user_id,),
            )
            room_rows = cursor.fetchall()
            rooms = {row["room_name"] for row in room_rows}
            return {
                "user_id": row["user_id"],
                "username": row["username"],
                "status": row["status"],
                "joined_at": row["joined_at"],
                "last_seen": row["last_seen"],
                "rooms": rooms,
            }

    def create_user(
        self,
        user_id: str,
        username: str,
        status: str = "online",
        joined_at: float | None = None,
        last_seen: float | None = None,
    ) -> None:
        """Create a new user"""
        import time

        if joined_at is None:
            joined_at = time.time()
        if last_seen is None:
            last_seen = time.time()
        with self.lock:
            cursor = self.conn.cursor()
            cursor.execute(
                """
                INSERT OR REPLACE INTO users (user_id, username, status, joined_at, last_seen)
                VALUES (?, ?, ?, ?, ?)
            """,
                (user_id, username, status, joined_at, last_seen),
            )
            self.conn.commit()

    def update_user(self, user_id: str, **kwargs) -> None:
        """Update user fields"""
        allowed_fields = {"username", "status", "last_seen"}
        updates = {k: v for k, v in kwargs.items() if k in allowed_fields}
        if not updates:
            return
        with self.lock:
            cursor = self.conn.cursor()
            set_clause = ", ".join(f"{k} = ?" for k in updates)
            values = list(updates.values()) + [user_id]
            cursor.execute(
                f"""
                UPDATE users
                SET {set_clause}
                WHERE user_id = ?
            """,
                values,
            )
            self.conn.commit()

    # Room methods

    def get_room(self, room_name: str) -> dict[str, Any] | None:
        """Get room by room_name"""
        with self.lock:
            cursor = self.conn.cursor()
            cursor.execute(
                """
                SELECT room_name, created_at, created_by, creator_username, is_public, user_count
                FROM rooms
                WHERE room_name = ?
            """,
                (room_name,),
            )
            row = cursor.fetchone()
            if row is None:
                return None
            return {
                "name": row["room_name"],
                "created_at": row["created_at"],
                "created_by": row["created_by"],
                "creator_username": row["creator_username"],
                "is_public": bool(row["is_public"]),
                "user_count": row["user_count"],
            }

    def create_room(
        self,
        room_name: str,
        created_by: str,
        creator_username: str,
        is_public: bool = True,
        created_at: float | None = None,
    ) -> None:
        """Create a new room"""
        import time

        if created_at is None:
            created_at = time.time()
        with self.lock:
            cursor = self.conn.cursor()
            cursor.execute(
                """
                INSERT OR REPLACE INTO rooms (room_name, created_at, created_by, creator_username, is_public, user_count)
                VALUES (?, ?, ?, ?, ?, 0)
            """,
                (room_name, created_at, created_by, creator_username, 1 if is_public else 0),
            )
            self.conn.commit()

    def update_room(self, room_name: str, **kwargs) -> None:
        """Update room fields"""
        allowed_fields = {"is_public"}
        updates = {k: v for k, v in kwargs.items() if k in allowed_fields}
        if not updates:
            return
        with self.lock:
            cursor = self.conn.cursor()
            # Convert boolean to integer for is_public
            processed_updates = {}
            for k, v in updates.items():
                if k == "is_public":
                    processed_updates[k] = 1 if v else 0
                else:
                    processed_updates[k] = v
            set_clause = ", ".join(f"{k} = ?" for k in processed_updates)
            values = list(processed_updates.values()) + [room_name]
            cursor.execute(
                f"""
                UPDATE rooms
                SET {set_clause}
                WHERE room_name = ?
            """,
                values,
            )
            self.conn.commit()

    def list_rooms(self, is_public: bool | None = None) -> list[dict[str, Any]]:
        """List rooms, optionally filtered by is_public"""
        with self.lock:
            cursor = self.conn.cursor()
            if is_public is not None:
                cursor.execute(
                    """
                    SELECT room_name, created_at, created_by, creator_username, is_public, user_count
                    FROM rooms
                    WHERE is_public = ?
                    ORDER BY created_at DESC
                """,
                    (1 if is_public else 0,),
                )
            else:
                cursor.execute(
                    """
                    SELECT room_name, created_at, created_by, creator_username, is_public, user_count
                    FROM rooms
                    ORDER BY created_at DESC
                """
                )
            rows = cursor.fetchall()
            return [
                {
                    "name": row["room_name"],
                    "created_at": row["created_at"],
                    "created_by": row["created_by"],
                    "creator_username": row["creator_username"],
                    "is_public": bool(row["is_public"]),
                    "user_count": row["user_count"],
                }
                for row in rows
            ]

    def increment_room_user_count(self, room_name: str) -> None:
        """Increment user count for a room"""
        with self.lock:
            cursor = self.conn.cursor()
            cursor.execute(
                """
                UPDATE rooms
                SET user_count = user_count + 1
                WHERE room_name = ?
            """,
                (room_name,),
            )
            self.conn.commit()

    def decrement_room_user_count(self, room_name: str) -> None:
        """Decrement user count for a room (with floor at 0)"""
        with self.lock:
            cursor = self.conn.cursor()
            cursor.execute(
                """
                UPDATE rooms
                SET user_count = MAX(0, user_count - 1)
                WHERE room_name = ?
            """,
                (room_name,),
            )
            self.conn.commit()

    # User-Room relationship methods

    def add_user_to_room(self, user_id: str, room_name: str) -> None:
        """Add user to a room"""
        with self.lock:
            cursor = self.conn.cursor()
            cursor.execute(
                """
                INSERT OR IGNORE INTO user_rooms (user_id, room_name)
                VALUES (?, ?)
            """,
                (user_id, room_name),
            )
            self.conn.commit()

    def remove_user_from_room(self, user_id: str, room_name: str) -> None:
        """Remove user from a room"""
        with self.lock:
            cursor = self.conn.cursor()
            cursor.execute(
                """
                DELETE FROM user_rooms
                WHERE user_id = ? AND room_name = ?
            """,
                (user_id, room_name),
            )
            self.conn.commit()

    def get_room_users(self, room_name: str) -> list[dict[str, Any]]:
        """Get all users in a room"""
        with self.lock:
            cursor = self.conn.cursor()
            cursor.execute(
                """
                SELECT u.user_id, u.username, u.status
                FROM users u
                INNER JOIN user_rooms ur ON u.user_id = ur.user_id
                WHERE ur.room_name = ?
                ORDER BY u.username
            """,
                (room_name,),
            )
            rows = cursor.fetchall()
            return [
                {
                    "user_id": row["user_id"],
                    "username": row["username"],
                    "status": row["status"],
                }
                for row in rows
            ]

    def get_user_rooms(self, user_id: str) -> set[str]:
        """Get all rooms a user belongs to"""
        with self.lock:
            cursor = self.conn.cursor()
            cursor.execute(
                """
                SELECT room_name
                FROM user_rooms
                WHERE user_id = ?
            """,
                (user_id,),
            )
            rows = cursor.fetchall()
            return {row["room_name"] for row in rows}

    def close(self):
        """Close the database connection"""
        with self.lock:
            self.conn.close()
