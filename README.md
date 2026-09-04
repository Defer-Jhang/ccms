## README

core\balps: 
- balps: Run task and message manager
- influxdb_mgr: Influxdb client API
- mssql_mgr: MSSQL client API
- meas_map_mgr: Query measurement, task, and device info map
- message_api: Handle xml message API
- message_mgr: Run xml message client
- reset_process: Reset watchdog status
- sync_time: Time synchronization
- task_mgr: Run multiple task
- watchdog: Monitor influxdb and balps

core\bin: 
- influxd: InfluxDB Server
- influx: InfluxDB Client
- telegraf: Telegraf binary

core\db: 
- init_db: Init DB
- ServerMap.csv: XMLServer uses data
- Watchdog.csv: Watchdog table data

core\grafana:
- CCMS Monitor v2.json: show system log, cell voltage/temperature, and xml request/reply

core\fc:
- FC1: run measurement function
- FC1.json: set input parameter and database output (table and column)

core\protocol
- wifi\meas: set system and get cell information

core\simulation
- ESPDevice: charge and monitor cell simulation
- XMLServer: central management for xml request server

## Execution CCMS System

- step 1: Set project root folder as ccms
- step 2: MSSQL database
  - Check DB alive
  - Create BALPS DB
  - run db initialization
    - Run db\init_db.py
      - Create ServerMap, Task, Watchdog tables and generate ServerMap and Watchdog data 
- step 3: run XMLServer terminal (python XMLServer.py)
- step 4: run watchdog terminal  (python watchdog.py)
  - Process InfluxDB and CCMS 
- step 5: run ESPDevice terminal (python ESPDevice.py)
- step 6: XMLServer terminal
  - Usage:
      - Command [Start_StoreHouse_ID] [End_StoreHouse_ID] [Charging Step]
  - Examples:

          > w2002 1 1 0   ...Set StoreHouseID 1 Config
          > w2003 1 1 1   ...Change to Charging Step 1
          > w2003 1 1 2   ...Change to Charging Step 2
          > w2003 1 1 3   ...Change to Charging Step 3

          > w2002 1 10 0  ...Set StoreHouseID 1-10 Config        
          > w2003 1 10 1  ...Change to Charging Step 1
          > w2003 1 10 2  ...Change to Charging Step 2