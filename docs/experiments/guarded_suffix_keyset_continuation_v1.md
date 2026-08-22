# Guarded suffix keyset continuation v1

Status: **rejected and retired negative result**. The historical implementation
identity `t16_s512_r512_e1_e2_e3_boundary_keyset_v1` is frozen in these
artifacts. Its GraphBench reference arm and experiment-specific telemetry have
been removed, and it was never part of production translation.
This confirmation is the canonical upstream record; later local reruns are not
part of the submitted evidence set.

The confirmation run used an isolated PostgreSQL 18.4 database with
`plan_cache_mode=auto`, 10 matched reload rounds, 20 warmups per round, and 50
measurements per arm per round (500 samples per arm). Intervals are paired
97.5% confidence intervals. The source artifact SHA-256 is
`e6aa00733de4861b9684d8f1276e922ff1e8059671e57400703a8266ca88ee25`.

| Case | Baseline p50 | Candidate p50 | Median ratio (97.5% CI) | Candidate shared hits | Interpretation |
| --- | ---: | ---: | ---: | ---: | --- |
| S511 | 11.254 ms | 4.758 ms | 0.416 [0.407, 0.439] | 6,866 | Existing bounded reverse branch wins |
| S512 | 11.165 ms | 4.748 ms | 0.428 [0.408, 0.453] | 6,879 | Existing bounded reverse branch wins |
| S513 | 11.247 ms | 20.065 ms | 1.791 [1.752, 1.875] | 54,020 | Continuation is 79% slower |
| S600 | 11.566 ms | 68.950 ms | 5.898 [5.649, 6.462] | 55,523 | Non-empty continuation is 490% slower |

S511 and S512 do not validate keyset continuation: they select the previously
known bounded reverse branch. S513 and S600 are the cases that exercise the new
continuation path, and both regress decisively. The reconstruction after the
prefix/remainder probes accounts for roughly 45,093 shared-buffer hits in both
overflow cases, so tuning the keyset predicate alone is not a credible next
step.

The experiment's unpublished resource gate v5 passed all 40 candidate records:
there was no temporary or
local workspace, WAL, sentinel-budget violation, or inactive-branch execution.
Correctness and structured-plan checks also passed under `auto`,
`force_custom_plan`, and `force_generic_plan`. This makes the rejection a
performance decision rather than a correctness or spill failure.

The compact machine-readable evidence is preserved in
`guarded_suffix_keyset_continuation_v1_pair.json` and
`guarded_suffix_keyset_continuation_v1_resources.json`. The JSON retains the
historical case names for artifact comparability; the active corpus uses
generic `GFSE-BOUNDARY-*` names for these fixed-suffix expansion holdouts.
