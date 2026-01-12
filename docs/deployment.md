# Deployment Guide

## Prerequisites

- AWS CLI configured
- Terraform >= 1.5.0
- kubectl configured for EKS
- Docker
- Go 1.21+

## Infrastructure Deployment

### 1. Deploy Infrastructure with Terraform

```bash
# Navigate to environment directory
cd infrastructure/envs/dev  # or staging/prod

# Initialize Terraform
terraform init

# Review plan
terraform plan

# Apply changes
terraform apply
```

### 2. Configure Kubernetes Access

```bash
# Update kubeconfig for EKS cluster
aws eks update-kubeconfig --name aegisai-eks-dev --region us-east-1
```

### 3. Deploy Services

#### Using Kustomize

```bash
# Deploy to dev
kubectl apply -k k8s/overlays/dev

# Deploy to staging
kubectl apply -k k8s/overlays/staging

# Deploy to prod
kubectl apply -k k8s/overlays/prod
```

#### Using Deployment Script

```bash
./scripts/deploy.sh dev
```

## CI/CD Deployment

Deployments are automated via GitHub Actions:

- **Dev**: Auto-deploys on push to `develop` branch
- **Staging**: Auto-deploys on push to `staging` branch
- **Prod**: Requires manual approval, deploys on push to `main` branch

## Manual Deployment Steps

1. Build Docker images
2. Push to container registry
3. Update Kubernetes manifests with new image tags
4. Apply manifests to cluster
5. Verify deployment status

## Rollback

```bash
# Rollback a deployment
kubectl rollout undo deployment/<service-name> -n aegisai-<env>

# Check rollout history
kubectl rollout history deployment/<service-name> -n aegisai-<env>
```
