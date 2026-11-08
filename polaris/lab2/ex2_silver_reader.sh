#!/usr/bin/env bash
# Exercise 2: catalog role `silver_reader` scoped to namespace `silver`,
# principal role `silver_analyst`, principal `silver_tester`. Run as root.
set -euo pipefail
API="$POLARIS_URI/api/management/v1"
C="$POLARIS_CATALOG"
TOKEN=$(curl -fsS "$POLARIS_URI/api/catalog/v1/oauth/tokens" \
  -d grant_type=client_credentials -d client_id=root -d client_secret=s3cr3t \
  -d scope=PRINCIPAL_ROLE:ALL | jq -r .access_token)
H=(-H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json")
call() { curl -sS -o /dev/null -w "%{http_code}" -X "$1" "${H[@]}" "$API$2" -d "$3"; }

echo "## create catalog role silver_reader: $(call POST "/catalogs/$C/catalog-roles" \
  '{"catalogRole":{"name":"silver_reader"}}')"
for P in NAMESPACE_LIST TABLE_LIST NAMESPACE_READ_PROPERTIES TABLE_READ_PROPERTIES TABLE_READ_DATA; do
  echo "## grant $P on namespace silver: $(call PUT "/catalogs/$C/catalog-roles/silver_reader/grants" \
    "{\"grant\":{\"type\":\"namespace\",\"namespace\":[\"silver\"],\"privilege\":\"$P\"}}")"
done
echo "## create principal role silver_analyst: $(call POST /principal-roles \
  '{"principalRole":{"name":"silver_analyst"}}')"
echo "## grant silver_reader to silver_analyst: $(call PUT \
  "/principal-roles/silver_analyst/catalog-roles/$C" '{"catalogRole":{"name":"silver_reader"}}')"
RESP=$(curl -fsS -X POST "${H[@]}" "$API/principals" \
  -d '{"principal":{"name":"silver_tester"},"credentialRotationRequired":false}')
echo "$RESP" | jq -r '"POLARIS_SILVER_TESTER_CREDENTIAL=\"\(.credentials.clientId):\(.credentials.clientSecret)\""' \
  >> /home/onyxia/.polaris-credentials.env
echo "## principal silver_tester created, credential appended to ~/.polaris-credentials.env"
echo "## assign silver_analyst to silver_tester: $(call PUT \
  /principals/silver_tester/principal-roles '{"principalRole":{"name":"silver_analyst"}}')"
echo "## silver_reader grants: $(curl -fsS "${H[@]}" \
  "$API/catalogs/$C/catalog-roles/silver_reader/grants" | jq -c '[.grants[] | .privilege]')"
