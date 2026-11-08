# Lab 09.1 - Deploy Polaris with Helm (k8s) - results

Date: 2026-11-08. Namespace / bucket / Polaris catalog: `user-a-charvet-ece`.

## Steps done (in lab order)

1. Patched the `vscode-pyspark-937556` StatefulSet: limits 2000m CPU / 2500Mi,
   requests 50m / 200Mi (was 2 CPU / 2000Mi, requests 200m / 400Mi).
   The pod was recreated; `~/work` (10Gi PVC) was kept.
2. Created `polaris/deployment/` (Chart.yaml, values.yaml, templates/bootstrap-job.yaml,
   templates/polaris-db-secret.yaml) exactly as in the lab (595 lines, md5 checked
   against the lab source).
3. Exported POLARIS_URI, POLARIS_MGMT_URI, POLARIS_CATALOG, API. Bronze check:
   `orders.csv` 331568 B, `users.csv` 7351 B.
4. Created ConfigMap `polaris-config`, Secrets `polaris-credential` and `polaris-s3`.
5. `helm upgrade --install polaris polaris/deployment --wait`: STATUS deployed,
   REVISION 1, exit code 0. Polaris pod Ready after about 2m30s.
6. Job `polaris-bootstrap`: Complete 1/1 in 38s, "Polaris RBAC bootstrap completed".
7. `curl http://polaris-mgmt:8182/q/health` -> `"status": "UP"`.
8. dev/analyst credentials written to `/home/onyxia/.polaris-credentials.env`
   (2 lines, chmod 600), not committed.

## Advanced section (REST API checks with the root token)

- Catalogs: one `INTERNAL` catalog `user-a-charvet-ece`,
  default-base-location `s3://user-a-charvet-ece/warehouse/`.
- Principals: `root`, `dev`, `analyst`.
- Principal roles: `data_analyst`, `service_admin`, `data_engineer`.
- Catalog roles: `catalog_admin`, `catalog_reader`, `catalog_contributor`.
- Credential reset endpoint: not called (it would invalidate the saved credentials).

## Deviations observed

- The lab's expected output lists a `data_scientist` principal role; the bootstrap
  Job actually creates `data_analyst` (that is what the Job script defines).
- `kubectl rollout status` stayed on "Waiting for partitioned roll out" because the
  command runs inside the pod being replaced; the web terminal froze and the page
  had to be reloaded. Verified afterwards: new pod Running with the new limits.
- `helm dependency update` downloads `polaris/deployment/charts/*.tgz`; these archives
  are not committed.
- `polaris-s3` holds the temporary STS credentials of the session: when they expire,
  Polaris must be refreshed as described in `09.polaris/help.md`.
- Teardown section not run: the lab says to keep Polaris for the next lab.
