# Example data

Both files are real telemetry from spacecraft in orbit.

## CYGNSS_F7_L0_2022_086_10_15_V01_F__first101pkts.tlm

The first 101 CCSDS Space Packets of a Level 0 file from the NASA CYGNSS F7 satellite, recorded on 25 March 2022, 7 APIDs in all. Taken unchanged from the test data of [CCSDSPy](https://github.com/ddasilva/ccsdspy) (`ccsdspy/tests/data/split/`).

The packets of APID 1313 carry the GPS week and GPS seconds as little-endian fields at user data offsets 4 and 6. Each value agrees with the UTC time in the same packet's secondary header. The example decodes those two fields only, because the rest of this packet's layout is not published with the data.

CCSDSPy is Copyright (c) 2018, Daniel da Silva, under the BSD 3-Clause licence. Its full text is at <https://github.com/ddasilva/ccsdspy/blob/main/LICENSE.rst>.

## opssat_ad_excerpt.csv

900 rows from `segments.csv` of OPSSAT-AD, the anomaly detection dataset for telemetry acquired on board the ESA OPS-SAT CubeSat: 300 timestamps of channels `CADC0872`, `CADC0873` and `CADC0874` from 27 January 2022. Rows and columns are unchanged. The dataset identifies channels by code, so MDP treats the values as they are published.

Ruszczak, B. (2024). OPSSAT-AD: anomaly detection dataset for satellite telemetry. Zenodo. <https://doi.org/10.5281/zenodo.12588359>. Licensed under [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/).
