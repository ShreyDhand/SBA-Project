from sqlalchemy.orm import DeclarativeBase, mapped_column
from sqlalchemy import Integer, String, DateTime, func


class Base(DeclarativeBase):
    pass

class Shot(Base):
    __tablename__ = "shots"

    id = mapped_column(Integer, primary_key=True)

    arena_id = mapped_column(String(250), nullable=False)
    batch_timestamp = mapped_column(DateTime, nullable=False)
    batch_count = mapped_column(Integer, nullable=False)
    trace_id = mapped_column(String(50), nullable=False)

    game_id = mapped_column(Integer, nullable=False)
    period = mapped_column(Integer, nullable=False)
    shot_type = mapped_column(String(250), nullable=False)
    game_time_seconds = mapped_column(Integer, nullable=False)
    shots_last_5_minutes = mapped_column(Integer, nullable=False)   
    date_created = mapped_column(DateTime, nullable=False, default=func.now())


    def to_dict(self):
        return {
            "trace_id": self.trace_id,
            "arena_id": self.arena_id,
            "batch_timestamp": self.batch_timestamp.isoformat() + "Z",
            "batch_count": self.batch_count,
            "game_id": self.game_id,
            "period": self.period,
            "shot_type": self.shot_type,
            "game_time_seconds": self.game_time_seconds,
            "shots_last_5_minutes": self.shots_last_5_minutes
        }

class Penalty(Base):
    __tablename__ = "penalties"

    id = mapped_column(Integer, primary_key=True)

    arena_id = mapped_column(String(250), nullable=False)
    batch_timestamp = mapped_column(DateTime, nullable=False)
    batch_count = mapped_column(Integer, nullable=False)
    trace_id = mapped_column(String(50), nullable=False)

    game_id = mapped_column(Integer, nullable=False)
    period = mapped_column(Integer, nullable=False)
    penalty_type = mapped_column(String(250), nullable=False)
    game_time_seconds = mapped_column(Integer, nullable=False)
    penalties_last_5_minutes = mapped_column(Integer, nullable=False)
    date_created = mapped_column(DateTime, nullable=False, default=func.now())


    def to_dict(self):
        return {
            "trace_id": self.trace_id,
            "arena_id": self.arena_id,
            "batch_timestamp": self.batch_timestamp.isoformat() + "Z",
            "batch_count": self.batch_count,
            "game_id": self.game_id,
            "period": self.period,
            "penalty_type": self.penalty_type,
            "game_time_seconds": self.game_time_seconds,
            "penalties_last_5_minutes": self.penalties_last_5_minutes
        }