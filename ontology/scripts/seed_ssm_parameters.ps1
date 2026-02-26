param(
    [Parameter(Mandatory = $false)]
    [string]$TenantId = "tenant-abc",
    [Parameter(Mandatory = $true)]
    [string]$PiiEncryptionKey
)

$ErrorActionPreference = "Stop"

$base = "/ai-ready/ontology/$TenantId"

Write-Host "Seeding SSM parameters under: $base"

aws ssm put-parameter --name "$base/domain-dictionary" --type String --value '{"version":"1.0","terms":[]}' --overwrite | Out-Null
aws ssm put-parameter --name "$base/pii-encryption-key" --type SecureString --value "$PiiEncryptionKey" --overwrite | Out-Null
aws ssm put-parameter --name "$base/freshness-thresholds" --type String --value '{"aging_days":90,"stale_days":365}' --overwrite | Out-Null
aws ssm put-parameter --name "$base/confidence-threshold" --type String --value '0.5' --overwrite | Out-Null
aws ssm put-parameter --name "$base/match-thresholds" --type String --value '{"exact":0.95,"probable":0.85,"ambiguous":0.60}' --overwrite | Out-Null
aws ssm put-parameter --name "$base/stopwords-ja" --type String --value '["もの","こと","ため","よう"]' --overwrite | Out-Null

Write-Host "SSM parameters have been updated."
