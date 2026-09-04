import json
import pandas as pd

# Load base panel config (from your provided JSON)
base_panel = {
    "datasource": {
        "type": "mssql",
        "uid": "fd9610ad-36b9-4755-bac9-b6b0fa7ae8e5"
    },
    "description": "",
    "fieldConfig": {
        "defaults": {
            "color": {
                "mode": "thresholds"
            },
            "decimals": 1,
            "fieldMinMax": False,
            "mappings": [],
            "thresholds": {
                "mode": "absolute",
                "steps": [
                    {
                        "color": "#747474"
                    }
                ]
            },
            "unit": "volt"
        },
        "overrides": [
            {
                "matcher": {
                    "id": "byName",
                    "options": "pallet_status"
                },
                "properties": [
                    {
                        "id": "mappings",
                        "value": [
                            {
                                "options": {
                                    "0": {
                                        "color": "#747474",
                                        "index": 0
                                    },
                                    "1": {
                                        "color": "light-blue",
                                        "index": 1
                                    },
                                    "2": {
                                        "color": "light-red",
                                        "index": 2
                                    }
                                },
                                "type": "value"
                            }
                        ]
                    }
                ]
            }
        ]
    },
    "gridPos": {
        "h": 2,
        "w": 2,
        "x": 0,
        "y": 0
    },
    "id": 266,
    "links": [
        {
            "targetBlank": True,
            "title": "",
            "url": "http://15.1.1.10:3000/d/feqogk55wni80d/ccms-cell-monitor?orgId=1&refresh=1s&from=now-5m&to=now&var-rack_id=1"
        }
    ],
    "options": {
        "colorMode": "background_solid",
        "graphMode": "none",
        "justifyMode": "center",
        "orientation": "auto",
        "percentChangeColorMode": "standard",
        "reduceOptions": {
            "calcs": [
                "allValues"
            ],
            "fields": "",
            "values": True
        },
        "showPercentChange": False,
        "textMode": "none",
        "wideLayout": True
    },
    "pluginVersion": "10.4.19+security-01",
    "targets": [
        {
            "dataset": "BALPS",
            "datasource": {
                "type": "mssql",
                "uid": "fd9610ad-36b9-4755-bac9-b6b0fa7ae8e5"
            },
            "editorMode": "code",
            "format": "table",
            "rawQuery": True,
            "rawSql": "SELECT pallet_id,\r\n       pallet_status\r\nFROM RackStatus\r\nWHERE rack_id = 1\r\n  AND (\r\n        pallet_status = 0\r\n        OR DATEDIFF(SECOND, last_update_time, GETDATE()) <= 15\r\n      )",
            "refId": "A",
            "sql": {
                "columns": [
                    {
                        "parameters": [],
                        "type": "function"
                    }
                ],
                "groupBy": [
                    {
                        "property": {
                            "type": "string"
                        },
                        "type": "groupBy"
                    }
                ],
                "limit": 50
            }
        }
    ],
    "title": "1",
    "type": "stat"
}

# Generate 84 panels in 7 rows, 12 per row
panels = []
for i in range(84):
    rack_id = i + 1
    row = i // 12
    col = i % 12
    x = col * 2
    y = row * 2

    panel = json.loads(json.dumps(base_panel))  # deep copy
    panel["title"] = f"H_{str(rack_id).zfill(3)}"
    panel["gridPos"]["x"] = x
    panel["gridPos"]["y"] = y
    panel["targets"][0]["rawSql"] = (
        f"SELECT pallet_id,\r\n       pallet_status\r\nFROM RackStatus\r\n"
        f"WHERE rack_id = {rack_id}\r\n  AND (\r\n        pallet_status = 0\r\n"
        f"        OR DATEDIFF(SECOND, last_update_time, GETDATE()) <= 15\r\n      )"
    )
    panel["links"][0]["url"] = (
        f"http://15.1.1.10:3000/d/feqogk55wni80d/ccms-cell-monitor"
        f"?orgId=1&refresh=1s&from=now-5m&to=now&var-rack_id={rack_id}"
    )
    panels.append(panel)

with open("generated_panels.json", "w", encoding="utf-8") as f:
    json.dump(panels, f, indent=2)
