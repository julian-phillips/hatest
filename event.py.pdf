from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

@dataclass
class Event:
	timestamp: datetime
	source: str
	entity: str
	event_type: str
	attributes: dict[str, Any] = field(default_factory=dict)
