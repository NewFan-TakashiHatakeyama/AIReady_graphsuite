# AI Ready Ontology

Infrastructure and schema project for the AI Ready Ontology pipeline.

## Environment-based CDK deploy

`cdk/environments.json` defines deploy parameters for `dev`, `stg`, and `prod`.

- `account`: AWS account ID
- `region`: deployment region
- `tenantId`: tenant identifier used for resource naming and parameters
- `stackPrefix`: stack name prefix
- `sharedVpcId`: existing shared VPC ID
- `alertEmail`: SNS alert destination
- `connectFileMetadataTableName`: Connect FileMetadata table name
- `connectFileMetadataStreamArn`: DynamoDB Stream ARN for FileMetadata (used by schemaTransform mapping)

Naming rule:
- Do not include environment labels like `dev`, `stg`, or `prod` in resource names.
- Keep `stackPrefix` as a neutral value such as `AIReadyOntology`.
- Separate environments by AWS account/region and `env` context, not by resource-name suffixes.

### 1) Update environment values

Edit `cdk/environments.json` and replace placeholder values:

- account IDs
- VPC IDs
- email addresses
- `connectFileMetadataStreamArn` stream label (`.../stream/<label>`)

### 2) Diff/Synth per environment

```powershell
cdk synth -c env=dev
cdk diff -c env=stg
```

### 3) Deploy per environment

```powershell
.\scripts\deploy_env.ps1 -Environment dev
.\scripts\deploy_env.ps1 -Environment stg -RequireApprovalNever
```

### 4) Optional: seed SSM parameters before deploy

```powershell
.\scripts\deploy_env.ps1 -Environment prod -SeedSsm -PiiEncryptionKey "<secure-key>"
```
