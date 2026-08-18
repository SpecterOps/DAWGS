# SP-I2 successor prospective power study V3

Status: prospective and pre-implementation. This study is the only permitted
P2 activity after the terminal `suffix-reverse-retry-v1` result. It does not
authorize a hidden-fan-in executor, selector, corpus fixture, database timing,
or protected-holdout access.

## Separation from terminal identities

`sp-i2-distance-v1` and `sp-i2-distance-v2` remain terminal. This study has
the distinct identity `sp-i2-distance-v3-power-study`; any later candidate
must receive its own V3 executor, policy, selector, rollback, corpus, and
evidence identities. Archived code and traces may calibrate a study but cannot
be rebound as V3 evidence.

## Archived calibration inputs

The sole inputs are the two clean, open V1 discovery traces from source
`3865cbc57758b7b20b7ffe431f27235873422eed`:

| Arm | Artifact SHA-256 | Structure |
| --- | --- | --- |
| S4 incumbent | `ac3ceb27ee92e3f4e21e3994ff9ee82d483b8081e9d44ddcef8e695ffdb1b6d0` | 20 balanced rounds, six open cases, 10 warm samples per record |
| I2 reference | `f6d79e81bdaafedaa95568d57140c14e0808fbb6fc261387abc916081137785a` | Same rounds, cases, and sample counts |

The study may use only their within-round timing distribution and empirical
round-drift vectors. It must not treat their old candidate outcome, old corpus,
or terminal V1/V2 gate disposition as a result for a future V3 candidate.

## Frozen design

The formal design tested by the study is 800 matched blocks, one pool session,
Repeatable Read, 25 ordinary warm-ups, and 100 timed samples per arm/case/block.
The two arms physically alternate incumbent/candidate then candidate/incumbent
across blocks; every order-stratum has the same number of blocks. The study
therefore provisions 80,000 timed observations per arm/case before separate
fresh-session, cancellation, resource, and holdout requirements.

The study keeps V2's 97.5% hierarchical interval and nearest-rank p95
semantics, 100,000 bootstrap draws for a later formal report, and its
95%-Wilson power decision rule. It must simulate at least 20,000 independent
draws for each of the following scenarios:

- A/A identity and the two 5% equivalence boundaries;
- target power at 0.90 median ratio and 0.97 p95 ratio, plus its boundary;
- control power at 1.00 median ratio and 0.97 p95 ratio, plus its boundary;
- odd and even order-stratum A/A power and their two 5% boundaries.

The candidate-side labels in the model are placeholders only. A pass requires
the Wilson lower decision-power bound to be at least 0.90 for every power
scenario, calibrated coverage to include 0.975, and false-pass upper bounds
of 0.015 for p95 boundaries and 0.0275 for median/control boundaries.

## Error model and feasibility threshold

The V2 calibration at 40 blocks and 100 timed samples estimated log standard
errors of 0.025959 pooled and 0.036712 by order stratum, and absolute standard
errors of 59.338us pooled and 83.917us by order stratum. The V3 simulator must
derive its prospective values by the fixed factor `sqrt(40 / 800)`, yielding
rounded-up bounds of 0.005806 and 0.008210 log units and 13.269us and 18.765us
respectively. It must resample all 800 blocks from the archived 20-round drift
vectors rather than repeat a fixed drift mean.

This block count is intentional: the 40-block V2 design made a two-sided 5%
A/A interval impossible, while the V3 order-stratum half-width is below the
5% log margin with additional room for estimator variation. Reducing blocks,
changing samples, pooling order strata, or changing thresholds creates a new
study identity.

## Required disposition

Implement a reproducible simulator under a V3-specific schema and domain
separator, verify both archive digests and all V3 constants, then commit its
report and test vectors. A failed simulation terminally stops this study before
any P2 executor or corpus work. A passing simulation authorizes only a fresh
V3 corpus and architecture-tournament protocol; it does not authorize a
candidate implementation, database timing, or a holdout.
