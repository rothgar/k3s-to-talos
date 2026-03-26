# AWS IAM Setup for EC2 Integration Tests

The EC2 integration workflow (`ec2-integration.yml`) authenticates to AWS using
[OIDC federation](https://docs.github.com/en/actions/security-for-github-actions/security-hardening-your-deployments/configuring-openid-connect-in-amazon-web-services)
— no long-lived access keys are stored in GitHub Secrets.

---

## Required GitHub Secrets

| Secret | Description |
|---|---|
| `AWS_ROLE_ARN` | Full ARN of the IAM role to assume (`arn:aws:iam::<account-id>:role/<role-name>`) |
| `AWS_VPC_ID` | VPC where test instances are launched (`vpc-xxxxxxxx`) |
| `AWS_SUBNET_ID` | Public subnet in that VPC — must auto-assign public IPs (`subnet-xxxxxxxx`) |

---

## 1. Create the IAM OIDC Identity Provider

This only needs to be done once per AWS account.

```bash
aws iam create-open-id-connect-provider \
  --url https://token.actions.githubusercontent.com \
  --client-id-list sts.amazonaws.com \
  --thumbprint-list 6938fd4d98bab03faadb97b34396831e3780aea1
```

---

## 2. Create the IAM Role

### Trust policy — `trust-policy.json`

Replace `<YOUR_GITHUB_ORG>` and `<YOUR_REPO_NAME>` with your values.

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Federated": "arn:aws:iam::<ACCOUNT_ID>:oidc-provider/token.actions.githubusercontent.com"
      },
      "Action": "sts:AssumeRoleWithWebIdentity",
      "Condition": {
        "StringEquals": {
          "token.actions.githubusercontent.com:aud": "sts.amazonaws.com"
        },
        "StringLike": {
          "token.actions.githubusercontent.com:sub": "repo:<YOUR_GITHUB_ORG>/<YOUR_REPO_NAME>:*"
        }
      }
    }
  ]
}
```

```bash
aws iam create-role \
  --role-name k3s-to-talos-ci \
  --assume-role-policy-document file://trust-policy.json
```

---

## 3. Attach the Permissions Policy

The workflow needs EC2 and SSM read access. Create the inline policy:

### `ec2-policy.json`

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "SSMReadOnly",
      "Effect": "Allow",
      "Action": [
        "ssm:GetParameter"
      ],
      "Resource": [
        "arn:aws:ssm:us-west-2::parameter/aws/service/canonical/ubuntu/*",
        "arn:aws:ssm:us-west-2::parameter/aws/service/debian/*"
      ]
    },
    {
      "Sid": "EC2DescribeImages",
      "Effect": "Allow",
      "Action": [
        "ec2:DescribeImages"
      ],
      "Resource": "*"
    },
    {
      "Sid": "EC2RunAndManage",
      "Effect": "Allow",
      "Action": [
        "ec2:RunInstances",
        "ec2:DescribeInstances",
        "ec2:TerminateInstances",
        "ec2:CreateSecurityGroup",
        "ec2:DeleteSecurityGroup",
        "ec2:AuthorizeSecurityGroupIngress",
        "ec2:DescribeSecurityGroups",
        "ec2:CreateTags"
      ],
      "Resource": "*",
      "Condition": {
        "StringEquals": {
          "aws:RequestedRegion": "us-west-2"
        }
      }
    }
  ]
}
```

```bash
aws iam put-role-policy \
  --role-name k3s-to-talos-ci \
  --policy-name ec2-integration-tests \
  --policy-document file://ec2-policy.json
```

---

## 4. Set GitHub Secrets

In your repository → **Settings → Secrets and variables → Actions**:

```
AWS_ROLE_ARN   = arn:aws:iam::<ACCOUNT_ID>:role/k3s-to-talos-ci
AWS_VPC_ID     = vpc-xxxxxxxx
AWS_SUBNET_ID  = subnet-xxxxxxxx
```

The subnet must be in **us-west-2** (matches `AWS_REGION` in the workflow) and must
have **Auto-assign public IPv4** enabled so the runner can SSH to the test instances.

---

## Cost notes

- Instance type: `t3.medium` (~$0.04/hr)
- Each workflow run launches up to 6 instances in parallel
- Instances are always terminated at the end of the job (including on failure)
- Estimated cost per full run: < $0.10 (instances typically run for 5–10 minutes)

---

## Restricting to specific branches (optional)

To prevent the workflow from running on untrusted forks, update the trust policy
condition to require `ref:refs/heads/main`:

```json
"StringEquals": {
  "token.actions.githubusercontent.com:sub": "repo:<ORG>/<REPO>:ref:refs/heads/main"
}
```

Or keep the `StringLike` with `*` to allow all branches (PRs from forks will not
have access to Secrets and therefore will not trigger the AWS auth step).
