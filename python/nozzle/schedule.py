from dataclasses import dataclass
from typing import Optional
from datetime import timedelta

@dataclass
class Schedule:
    interval: timedelta
    start_block: Optional[int] = None
    end_block: Optional[int] = None

    @classmethod
    def daily(cls, start_block: Optional[int] = None, end_block: Optional[int] = None):
        return cls(interval=timedelta(days=1), start_block=start_block, end_block=end_block)

    @classmethod
    def hourly(cls, start_block: Optional[int] = None, end_block: Optional[int] = None):
        return cls(interval=timedelta(hours=1), start_block=start_block, end_block=end_block)

    @classmethod
    def weekly(cls, start_block: Optional[int] = None, end_block: Optional[int] = None):
        return cls(interval=timedelta(weeks=1), start_block=start_block, end_block=end_block)
    
    @classmethod
    def monthly(cls, start_block: Optional[int] = None, end_block: Optional[int] = None):
        return cls(interval=timedelta(days=30), start_block=start_block, end_block=end_block)
    
    @classmethod
    def minute(cls, start_block: Optional[int] = None, end_block: Optional[int] = None):
        return cls(interval=timedelta(minutes=1), start_block=start_block, end_block=end_block)
    
    