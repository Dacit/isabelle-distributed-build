# Code and Data for: Distributed Parallel Build for the Isabelle Archive of Formal Proofs

This is a repository containing source code and data for the paper:

```bibtex
@inproceedings{Huch:2214:DPBIAFP,
  author = {Huch, Fabian and Wenzel, Makarius},
  booktitle = {{Interactive Theorem Proving (ITP 2024)}},
  title = {{Distributed Parallel Build for the Isabelle Archive of Formal Proofs}},
  publisher = {{Schloss Dagstuhl -- Leibniz-Zentrum f{\"u}r Informatik}},
  series = {{LIPIcs}},
  doi = {10.4230/LIPIcs.ITP.2024.29},
  year = {2024}
}
```

## Usage

This repository contains two entries: [preliminary](preliminary) with data and code for the
preliminary experiments, and [experiments](experiments) for the Experimental Results section.
All commands/paths given in this README are relative to the repository root. We also assume a
working Isabelle (usable via `isabelle` command) + afp-devel installation in the specified revision.

Both entries require to copy the [registry.toml](registry.toml) file to the `etc` dir in the
Isabelle user home (e.g., `~/.isabelle/etc/`), and adding the component with
`isabelle component -u <DIR>` (either `preliminary` or `experiments` as `<DIR>`) after the other
steps are finished. The component can be removed again with `isabelle component -x <DIR>`.

You'll need a running PostgreSQL 14 database with `isabelle_build` database.
Via docker:
```shell
docker run --name postgres -e POSTGRES_PASSWORD=mysecretpassword -d -p 5432:5432 postgres:14
docker exec -it postgres bash
su - postgres
psql
CREATE DATABASE isabelle_build;
```
Isabelle needs to be configured to use the db. For the docker example, set in `~/.isabelle/etc/preferences`:
```
build_database_name = "isabelle_build"
build_database_host = "localhost"
build_database_port = "5432"
build_database_user = "postgres"
build_database_password = "mysecretpassword"
```

### Preliminary

Requires `Isabelle/08b83f91a1b2` with `AFP/f51730d25fc3`. Experiments can be run with:

- `isabelle analyze_overhead`: analyzes the previous Isabelle build for imports overhead
- `isabelle host_factors`: data for Figure 2
- `isabelle thread_curves`: data for Figure 3
- `isabelle interpolation_factors`: data for Figure 4
- `isabelle compare_pth_params`: data for Figure 5

### Experiments

Requires linux, `Isabelle/Isabelle2024` with `AFP/Isabelle2024`, and a
working `IBM ILOG CPLEX 22.11` installation, with the environment variable `CPLEX_STUDIO_DIR2211`
set to its path.

Some experiments need a dataset parameter parameter, which corresponds to:

| DATASET | Description                            |
|---------|----------------------------------------|
| B       | Heterogeneous                          |
| B1      | Heterogeneous, only 4 fastest machines |
| C       | Release                                |
| C2      | Release, only 4 fastest machines       |

- `isabelle build_schedule_offline <DATASET>`: Generate schedule by heuristic for Figure 7
- `isabelle cplex_lowerbound <DATASET>`: Compute lower bound (by CPLEX in 3h) for Figure 7
- `isabelle cplex_solve_problem <DATASET>`: Generate schedule by CPLEX in 3h for Figure 7
- `isabelle cplex_solve_problem_fast <DATASET>`: Generate schedule by CPLEX in 5m for Figure 7
- `isabelle schedule_analysis CA`: data for Figure 8
