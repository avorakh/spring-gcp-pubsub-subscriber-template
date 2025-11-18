# spring-gcp-rest-api-template

spring-gcp-rest-api-template

```bash
curl -v 'http://localhost:8090/template/app/info'
```

## Google Cloud CLI Configuration with Service Account Key

This guide explains how to configure the **Google Cloud CLI (gcloud)** with a **service account key** for **macOS**, **Linux (Ubuntu)**, and **Windows (PowerShell)**. The environment variables will be saved in profile files to persist across sessions.

---

### 1. Create a secure directory for your service account key

#### macOS / Linux (Ubuntu)

```bash
mkdir -p $HOME/.security
mv my-service-account-key.json $HOME/.security/
```

#### Windows (PowerShell)

```powershell
New-Item -ItemType Directory -Path "$env:USERPROFILE\.security" -Force
Move-Item .\my-service-account-key.json "$env:USERPROFILE\.security\"
```

---

### 2. Set environment variables (persistent)

#### macOS / Linux (Ubuntu)

1. Open your shell profile file (`~/.bashrc` or `~/.zshrc`):

```bash
nano ~/.bashrc   # or ~/.zshrc for zsh
```

2. Add the following lines:

```bash
# Google Cloud SDK
export CLOUDSDK_PYTHON=python3
export GOOGLE_CLOUD_PROJECT="sample-app-123456"
export GOOGLE_APPLICATION_CREDENTIALS="$HOME/.security/my-service-account-key.json"
```

3. Save the file and reload the profile:

```bash
source ~/.bashrc   # or source ~/.zshrc
```

---

#### Windows (PowerShell)

1. Open PowerShell profile:

```powershell
notepad $PROFILE
```

> If `$PROFILE` does not exist, create it:

```powershell
New-Item -ItemType File -Path $PROFILE -Force
```

2. Add the following lines to the profile:

```powershell
# Google Cloud SDK
$env:CLOUDSDK_PYTHON = "python3"
$env:GOOGLE_CLOUD_PROJECT = "sample-app-123456"
$env:GOOGLE_APPLICATION_CREDENTIALS = "$env:USERPROFILE\.security\my-service-account-key.json"
```

3. Save the file and reload the profile:

```powershell
. $PROFILE
```

---

### 3. Verify Application Default Credentials (ADC)

```bash
gcloud auth application-default print-access-token
gcloud auth list
```

> If a token prints successfully, ADC is properly configured.
> ADC is used by SDKs (Python, Java ,Node.js, Go, etc.) and Terraform.

---
