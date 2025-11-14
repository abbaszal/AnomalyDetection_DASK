```text
 ┌───────────────────────────────────────────────────────────────┐
 │                         RAW INPUT                             │
 │       Long-format Dask DataFrame (df_eng)                     │
 │   columns: [timestamp index, hwid, metric, value]             │
 └───────────────────────────────────────────────────────────────┘
                         |
                         | sort_index() per partition
                         v
 ┌───────────────────────────────────────────────────────────────┐
 │                RESAMPLE PER PARTITION                         │
 │         map_partitions(resample_partition)                    │
 │                                                               │
 │ groupby(["hwid", "metric"])                                   │
 │ resample("1min").last()                                       │
 │ → 1-minute time series per hwid+metric                        │
 │                                                               │
 │ Output: df_eng_resampled                                      │
 │ columns: [hwid, metric, timestamp, value]                     │
 └───────────────────────────────────────────────────────────────┘
                         |
                         | pivot to wide format
                         v
 ┌───────────────────────────────────────────────────────────────┐
 │                      WIDE FORMAT TABLE                        │
 │            map_partitions(to_wide) + meta                     │
 │                                                               │
 │ pivot:                                                        │
 │   index = ["timestamp","hwid"]                                │
 │   columns = metric                                            │
 │   values = value                                              │
 │                                                               │
 │ → df_eng_wide                                                 │
 │ columns:                                                      │
 │   [timestamp, hwid, metric0, metric1, metric2, ...]           │
 └───────────────────────────────────────────────────────────────┘
                         |
                         | compute transitions per metric
                         v
 ┌───────────────────────────────────────────────────────────────┐
 │              COMPUTE TRANSITIONS PER METRIC                   │
 │        map_partitions(compute_engine_transitions)             │
 │                                                               │
 │ For each metric M:                                            │
 │   M_transitions = | M(t) - M(t-1) |   (per hwid)              │
 │                                                               │
 │ → df_eng_with_trans                                           │
 │ columns:                                                      │
 │   [timestamp, hwid, M..., M_transitions...]                   │
 └───────────────────────────────────────────────────────────────┘
                         |
                         | aggregate hourly
                         v
 ┌───────────────────────────────────────────────────────────────┐
 │                   PER-HOUR AGGREGATION                        │
 │           map_partitions(per_hour_aggregation)                │
 │                                                               │
 │ For each hwid and hour:                                       │
 │   metric mean      = avg(M)                                   │
 │   transitions sum  = sum(M_transitions)                       │
 │                                                               │
 │ → df_eng_hourly                                               │
 │ columns:                                                      │
 │   [hwid, timestamp_hour, mean(M), sum(transitions)]           │
 └───────────────────────────────────────────────────────────────┘
                         |
                         | flag anomalies
                         v
 ┌───────────────────────────────────────────────────────────────┐
 │                       ANOMALY FLAGGING                        │
 │            map_partitions(flag_anomalies)                     │
 │                                                               │
 │ For each metric M:                                            │
 │   M_anomaly = M_transitions > TRANSITION_THRESHOLD            │
 │                                                               │
 │ Device-hour anomaly:                                          │
 │   any_engine_anomaly = OR across all M_anomaly columns        │
 │                                                               │
 │ → df_eng_hourly_anom                                          │
 │ columns:                                                      │
 │   hwid, timestamp_hour, metrics, transitions, anomalies       │
 └───────────────────────────────────────────────────────────────┘


```
