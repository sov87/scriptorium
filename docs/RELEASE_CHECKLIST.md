# Release Checklist (Snapshot)

1) Run doctor (strict)
2) Run rights_lint.py (must be ok)
3) Rebuild db + vectors
4) Run one retrieve sanity query
5) If LM Studio used, run one answer-db sanity query
6) Create snapshot zip
7) Verify snapshot contains src/, configs/, docs/, db/ (and canon only when distributable)