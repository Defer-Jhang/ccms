# CCMS function-test automation

The suite models `XMLServer -> CCMS -> ESPDevice` without requiring SQL
Server, Redis, InfluxDB, or live sockets. It reads the complete mapping from
`CCMS_MAPPING_CSV`, then `C:\Users\defer\Downloads\mapping.csv`, and finally
`core/db/ServerMap.csv`.

Cross-round assignments are data-driven by `tests/fixtures/cycle_assignments.csv`;
the fixture intentionally includes C00001/C00002 moving from Storehouse 1 to 7
and a C00001/C00009 pairing in another round.

Run from the repository root:

```powershell
.\.venv\Scripts\python.exe -m unittest discover -s tests -v
```

The test cases override unittest formatting, so the standard command above also
shows only the short name. An equivalent compact runner is available when you
want the test suite entry point to be explicit:

```powershell
.\.venv\Scripts\python.exe tests\run_function_tests.py
```

## Selecting cases

Edit `ENABLED_CASES` near the top of `tests/run_function_tests.py`:

```python
ENABLED_CASES = ["W1001->W2005->W2004", "C03 W2004 OK/NG/Water"]
```

The list accepts either a short name from the table below or the complete
`test_...` function name. An empty list (`ENABLED_CASES = []`) runs all cases.

## Test short-name map

| Short name | Meaning / expected condition | Test function |
|---|---|---|
| MAP-01 | 驗證 1008 rows、84 Storehouse、每倉 L/R 各 6 positions，複合鍵不可重複 | `test_mapping_is_complete_and_structurally_valid` |
| MAP-02 | QRCode 可重複，但 `(SerialBoardID, Position)` 必須唯一 | `test_mapping_duplicate_qrcode_values_are_not_used_as_identity` |
| MAP-03 | C00001/C00002 可由 Storehouse 1 移到 Storehouse 7，W2002 ID 必須更新 | `test_mapping_cycle_can_move_same_pair_to_another_storehouse` |
| MAP-04 | 同一 cycle 將同一 SerialBoard 放入兩個 Storehouse 必須拒絕 | `test_mapping_rejects_duplicate_active_serial_in_one_cycle` |
| MAP-05 | cycle fixture 可產生 C00001/C00009 等任意 L/R 配對 | `test_cycle_assignment_fixture_supports_arbitrary_pairs` |
| C01-1 | 時間差在 10 秒內通過；30 秒差距及錯誤格式拒絕；W0001 欄位完整 | `test_c01_time_sync_boundary_and_invalid_input`<br>`test_c01_time_sync_and_alive_xml_contract` |
| C01-2 | W0002 必須包含 AliveRequest、TransactionID、TRX_ID、LINE_ID、ReturnCode=0 | `test_c01_time_sync_and_alive_xml_contract` |
| C01-3 | 只有左右兩個 Task 都 ready 才通過；只有一個 ready 必須 timeout | `test_c01_task_ready_requires_both_serial_tasks` |
| C01-4 | MPD heartbeat 未滿 60 秒不可清 Task；達 60 秒必須清理 | `test_c01_mpd_heartbeat_timeout_uses_60_second_threshold` |
| C02-1 | W2002 必須有 3 個 RecipeStep；Protect key 少、多、大小寫錯時整筆拒絕 | `test_c02_w2002_has_three_steps_and_all_c07_thresholds`<br>`test_c02_w2002_config_parser_accepts_exact_recipe_and_rejects_whole_request_on_mismatch` |
| C02-1 | W2002 large recipe 必須有 15 個工步；前 14 步 START_CC/REST 交替，第 15 步固定 END | `test_c02_w2002_large_recipe_alternates_15_steps_and_ends_end` |
| C02-2 | REST、CC、END 三種步驟切換回覆應為 StoreHouseStepCheckReply / OK | `test_c02_w2003_switch_protection_parameter_reply` |
| C03 | W2004 ReturnCode 優先順序為 Water > NG > OK，回覆 NGStatus 正確 | `test_c03_w2004_status_priority_ok_ng_water` |
| C04 | 缺少或超過 timeout 的 ESP 狀態必須轉成 NoResponse / ReturnCode=3 | `test_c04_w2004_missing_rack_is_no_response`<br>`test_w2004_stale_w3001_status_becomes_no_response` |
| C05 | W2005 左右都成功回 ReturnCode=0；任一停止失敗回 ReturnCode=1 | `test_c05_w2005_stop_reply_ok_and_exception` |
| C06 | 必須先完成舊 Storehouse W2005，再切換新 cycle 並送 W2002 | `test_c06_two_rack_two_round_sequence_changes_assignments_only_after_stop` |
| C07-1..13 | W2002 內所有電壓、電流、溫度、斜率、延遲門檻符合測試表 | `test_c07_threshold_values_match_function_test_table`<br>`test_c07_remaining_wire_and_delay_thresholds` |
| W1001 | ESP 連續 30 秒回 NG，第 31 筆 ReturnCode=2 才升級 Water；高溫欄位本身不可升級 | `test_w1001_alarm_report_is_driven_by_esp_return_code` |
| W1001->W2005->W2004 | 驗證 AlarmStopReport → JudgmentCompletionNotificationReply → StoreHouseNGCheckReply 順序 | `test_w1001_w2005_w2004_message_order_is_explicit` |
| Protect | 完整 key、重新排序、空值可測；未知/缺少/大小寫錯/重複 key 必須拒絕 | `test_protect_parameter_exact_key_order_is_accepted`<br>`test_protect_parameter_reordered_xml_keys_are_accepted`<br>`test_known_empty_protect_value_is_a_disabled_parameter`<br>`test_protect_parameter_mismatch_combinations_reject_whole_step`<br>`test_protect_config_key_mismatch_combinations_reject`<br>`test_protect_parameter_mismatch_in_second_step_rejects_config_request`<br>`test_protect_parameter_duplicate_xml_key_is_rejected`<br>`test_protect_parameter_duplicate_xml_key_with_different_values_is_rejected` |
| Other | 未知 MessageName 不可被轉成任何 W 操作，也不可寫入有效 cache | `test_unknown_message_mapping_is_not_silently_routed` |

When a normal assertion fails, the runner reports the short name followed by
the failed condition, for example:

```text
C01-3 both tasks ready: condition failed
```

Covered cases include the original C01-C07 items, all Protect-parameter key
mismatch combinations, arbitrary L/R Storehouse assignments across cycles,
mapping integrity (84 Storehouses / 168 boards / 1008 rows), W1001 NG and
temperature Water escalation, W2005 stop acknowledgement, and W2004
OK/NG/Water/NoResponse status aggregation.

The two duplicate-XML-key tests now verify that the parser rejects both repeated
keys with the same value and repeated keys with different values. The error
message identifies which duplicate-value case was detected.

Each test has a short description shown by `-v`. Assertion failures are prefixed
with that description, for example `C01-3 both tasks ready: condition failed`,
so the failed condition is visible without reading the source first.

## E2E 測試

`test_e2e_ccms_flow.py` 是獨立的流程測試，驗證完整資料鏈：

`XMLServer request -> CCMS message_api -> TaskManager/Redis -> FlowControl -> ESPDevice JSON -> CCMS XML reply`

它會使用 `XMLSocketServer` 的正式 W2002/W2004 request builder、正式的
`message_api`、`FC_api` 與 `meas_api`，並透過 localhost 真實 TCP socket 傳送
XML 與 ESP framed JSON。SQL Server、Redis、InfluxDB 則在測試內以可重現的
記憶體替身隔離，不需要啟動外部服務。

目前涵蓋三個結果分支：

| Short name | 驗證內容 | Test function |
|---|---|---|
| E2E W2002 ACK | W2002 建立兩個 Task，兩個 ESP 回 ACK，CCMS 回 StoreHouseStatusReply | `test_e2e_ccms_flow_ack` |
| E2E DATA->W2004 | ESP 回 DATA，CCMS 更新 rack status，W2004 回 OK | `test_e2e_ccms_flow_data_then_w2004` |
| E2E ALARM->W1001 | ESP 回 ALARM code 2，CCMS 回 Water AlarmStopReport | `test_e2e_ccms_flow_alarm` |
| E2E NG->W2005->W2004 | ESP 回 NG，依序執行 W1001、W2005、W2004，最後回 NG | `test_e2e_ccms_flow_ng_w2005_w2004` |
| E2E W1001 NG->Water | ESP 先連續回報 NG，再升級 Water，並驗證後續 W2005/W2004 | `test_e2e_ccms_flow_ng_escalates_to_water` |

只執行 E2E：

```powershell
.\.venv\Scripts\python.exe -m unittest tests.test_e2e_ccms_flow -v
```

## Test-item call paths

The following table has one row per test method. The `->` arrow means “the
next function called”. A path under `tests/...` is a test helper or test
adapter; a path under `core/...` is the CCMS/production implementation.

### Function and integration tests

| ID | Test function | Call path |
|---|---|---|
| MAP-01 | `test_mapping_is_complete_and_structurally_valid` | `setUpClass -> load_mapping_rows -> mapping_path -> mapping_frame` (`tests/test_functional_automation.py`) |
| MAP-02 | `test_mapping_duplicate_qrcode_values_are_not_used_as_identity` | `setUpClass -> load_mapping_rows -> mapping_path -> mapping_frame` (`tests/test_functional_automation.py`) |
| MAP-03 | `test_mapping_cycle_can_move_same_pair_to_another_storehouse` | `materialize_cycle -> mapping_frame -> XMLSocketServer.create_request_w2002_xml` (`tests/test_functional_automation.py` -> `core/simulation/XMLServer.py`) |
| MAP-04 | `test_mapping_rejects_duplicate_active_serial_in_one_cycle` | `materialize_cycle` (`tests/test_functional_automation.py`) |
| MAP-05 | `test_cycle_assignment_fixture_supports_arbitrary_pairs` | `csv.DictReader(cycle_assignments.csv) -> materialize_cycle` (`tests/test_functional_automation.py`) |
| C01-1 | `test_c01_time_sync_boundary_and_invalid_input` | `sync_time_api.verify_time_sync` (`core/balps/sync_time.py`) |
| C01-1/C01-2 | `test_c01_time_sync_and_alive_xml_contract` | `message_api.create_request_w0001_xml/create_request_w0002_xml -> message_api.generate_transaction_id` (`core/balps/message_api.py`) |
| C01-3 | `test_c01_task_ready_requires_both_serial_tasks` | `TaskManager.check_task_ready -> MSSQL.query_db_pd` (`core/balps/task_mgr.py` -> test fake in `tests/test_functional_automation.py`) |
| C01-4 | `test_c01_mpd_heartbeat_timeout_uses_60_second_threshold` | `FC_api.esp_heartbeat_check -> mssql_obj.clear_task_table` (`core/fc/FlowControl.py` -> test fake) |
| C02-1 | `test_c02_w2002_has_three_steps_and_all_c07_thresholds` | `XMLSocketServer.create_request_w2002_xml` (`core/simulation/XMLServer.py`) |
| C02-1 | `test_c02_w2002_config_parser_accepts_exact_recipe_and_rejects_whole_request_on_mismatch` | `XMLSocketServer.create_request_w2002_xml -> meas_map_api.parse_config_info -> meas_map_api.parse_protect_params` (`core/simulation/XMLServer.py` -> `core/balps/meas_map_mgr.py`) |
| C02-1 | `test_c02_w2002_large_recipe_alternates_15_steps_and_ends_end` | `XMLSocketServer.create_request_w2002_xml_large -> meas_map_api.parse_config_info -> meas_map_api.parse_protect_params` (`core/simulation/XMLServer.py` -> `core/balps/meas_map_mgr.py`) |
| C02-2 | `test_c02_w2003_switch_protection_parameter_reply` | `msg_aggeregation.create_reply_w2003_xml -> get_request_value -> get_ng_status` (`core/balps/system_cache.py`) |
| C03 | `test_c03_w2004_status_priority_ok_ng_water` | `msg_aggeregation.create_reply_w2004_xml -> get_request_value -> get_ng_status` (`core/balps/system_cache.py`) |
| C04 | `test_c04_w2004_missing_rack_is_no_response` | `cache_api.handle_rack_status -> msg_cache.build_pallet_json/update_cache -> msg_aggeregation.create_reply_w2004_xml` (`core/balps/system_cache.py`) |
| C05 | `test_c05_w2005_stop_reply_ok_and_exception` | `msg_aggeregation.create_reply_w2005_xml -> get_request_value -> get_ng_status` (`core/balps/system_cache.py`) |
| C06 | `test_c06_two_rack_two_round_sequence_changes_assignments_only_after_stop` | `materialize_cycle` and event-order assertions (`tests/test_functional_automation.py`) |
| C07-1..13 | `test_c07_threshold_values_match_function_test_table` | `XMLSocketServer.create_request_w2002_xml` (`core/simulation/XMLServer.py`) |
| C07 | `test_c07_remaining_wire_and_delay_thresholds` | `XMLSocketServer.create_request_w2002_xml` (`core/simulation/XMLServer.py`) |
| W1001 | `test_w1001_alarm_report_is_driven_by_esp_return_code` | `msg_aggeregation.create_request_w1001_xml -> get_ng_status` (`core/balps/system_cache.py`) |
| W1001->W2005->W2004 | `test_w1001_w2005_w2004_message_order_is_explicit` | `create_request_w1001_xml -> create_reply_w2005_xml -> create_reply_w2004_xml` (`core/balps/system_cache.py`) |
| Protect | `test_protect_parameter_exact_key_order_is_accepted` | `recipe_step_xml -> meas_map_api.parse_protect_params -> get_required_text` (`tests/test_functional_automation.py` -> `core/balps/meas_map_mgr.py`) |
| Protect | `test_protect_parameter_reordered_xml_keys_are_accepted` | `recipe_step_xml -> meas_map_api.parse_protect_params -> get_required_text` (`tests/test_functional_automation.py` -> `core/balps/meas_map_mgr.py`) |
| Protect | `test_known_empty_protect_value_is_a_disabled_parameter` | `recipe_step_xml -> meas_map_api.parse_protect_params` (`tests/test_functional_automation.py` -> `core/balps/meas_map_mgr.py`) |
| Protect | `test_protect_parameter_mismatch_combinations_reject_whole_step` | `recipe_step_xml -> meas_map_api.parse_protect_params` (key/case/value validation in `core/balps/meas_map_mgr.py`) |
| Protect | `test_protect_config_key_mismatch_combinations_reject` | `recipe_step_xml -> meas_map_api.parse_protect_params` (XML/config key comparison in `core/balps/meas_map_mgr.py`) |
| Protect | `test_protect_parameter_mismatch_in_second_step_rejects_config_request` | `recipe_step_xml x3 -> meas_map_api.parse_config_info -> parse_protect_params` (`tests/test_functional_automation.py` -> `core/balps/meas_map_mgr.py`) |
| Protect | `test_protect_parameter_duplicate_xml_key_is_rejected` | `recipe_step_xml(duplicate_key) -> meas_map_api.parse_protect_params` (duplicate-key detection in `core/balps/meas_map_mgr.py`) |
| Protect | `test_protect_parameter_duplicate_xml_key_with_different_values_is_rejected` | `recipe_step_xml(duplicate_key) -> meas_map_api.parse_protect_params` (duplicate-value comparison in `core/balps/meas_map_mgr.py`) |
| Other | `test_unknown_message_mapping_is_not_silently_routed` | `cache_api.map_message_to_operation` and `cache_api.handle_esp_data -> map_message_to_operation` (`core/balps/system_cache.py`) |
| C04 | `test_w2004_stale_w3001_status_becomes_no_response` | `cache_api.update_rack_status -> msg_cache.update_rack_info -> cache_api.handle_rack_status -> get_rack_info/build_pallet_json/update_cache` (`core/balps/system_cache.py`) |

### E2E tests

All E2E rows share this setup and teardown: `CCMSFixtureE2ETest.setUp ->
E2ERuntime.__init__/start -> message_api.__init__/run` and
`CCMSFixtureE2ETest.tearDown -> E2ERuntime.close` (`tests/test_e2e_ccms_flow.py`).
The CCMS production functions are real. `FrontendEndpoint` replaces the
XMLServer TCP peer and `ESPProtocolClient` replaces the ESPDevice TCP peer;
both adapters are defined in the E2E test file. SQL, Redis, and InfluxDB are
deterministic in-memory test doubles.

| Short name | Test function | Test-specific call path after the common setup |
|---|---|---|
| E2E W2002 ACK | `test_e2e_ccms_flow_ack` | `E2ERuntime.send_w2002 -> XMLSocketServer.create_request_w2002_xml -> FrontendEndpoint.send -> message_api.receive_response/parse_and_handle_messages -> TaskManager.check_task_exists/create_task -> FC_api.init_algorithm/process_xml_request/run_algorithm -> meas_api.run_meas -> ESPProtocolClient._serve/_send(ACK) -> cache_api.handle_esp_data/process_msg -> msg_aggeregation.create_reply_w2002_xml -> message_api.send_request -> FrontendEndpoint.receive` |
| E2E DATA->W2004 | `test_e2e_ccms_flow_data_then_w2004` | Common W2002 path -> `ESPProtocolClient._serve/_send(ACK,Data) -> FC_api.run_algorithm -> cache_api.handle_esp_data/update_rack_status` -> `E2ERuntime.send_w2004 -> XMLSocketServer.create_request_w2004_xml -> message_api.parse_and_handle_messages -> cache_api.handle_rack_status -> msg_aggeregation.create_reply_w2004_xml -> message_api.send_request -> FrontendEndpoint.receive` |
| E2E ALARM->W1001 | `test_e2e_ccms_flow_alarm` | Common W2002 path -> `ESPProtocolClient._wait_for_alarm/_alarm_payload -> FC_api.run_algorithm(ALARM) -> cache_api.handle_esp_data -> msg_aggeregation.create_request_w1001_xml -> message_api.send_request -> FrontendEndpoint.receive` |
| E2E NG->W2005->W2004 | `test_e2e_ccms_flow_ng_w2005_w2004` | Common W2002 path -> `ESPProtocolClient._alarm_payload(code=1) -> FC_api.run_algorithm -> cache_api.handle_esp_data -> create_request_w1001_xml` -> `E2ERuntime.send_w2005 -> XMLSocketServer.create_request_w2005_xml -> message_api.parse_and_handle_messages -> TaskManager.delete_task -> cache_api.handle_rack_stop/handle_esp_data -> create_reply_w2005_xml -> send_request` -> `E2ERuntime.send_w2004 -> XMLSocketServer.create_request_w2004_xml -> cache_api.handle_rack_status -> create_reply_w2004_xml -> send_request/FrontendEndpoint.receive` |
| E2E W1001 NG->Water | `test_e2e_ccms_flow_ng_escalates_to_water` | Common W2002 path -> `ESPProtocolClient._alarm_payload(code=1) -> FC_api.run_algorithm -> create_request_w1001_xml(NG)` -> `ESPProtocolClient._alarm_payload(code=2) -> FC_api.run_algorithm -> create_request_w1001_xml(Water)` -> W2005 path (`send_w2005 -> parse_and_handle_messages -> handle_rack_stop -> create_reply_w2005_xml`) -> W2004 path (`send_w2004 -> parse_and_handle_messages -> handle_rack_status -> create_reply_w2004_xml`) |

This table describes the currently implemented call path; it is also the
quickest place to see whether a case is a direct function test or a complete
XMLServer-to-CCMS-to-ESPDevice E2E test.

自訂 runner 的 `ENABLED_CASES` 已包含上述測試名稱，因此一般執行
`tests\run_function_tests.py` 也會一併執行 E2E。
