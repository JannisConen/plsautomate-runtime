# yesautomate-runtime

Runtime engine for [PlsAutomate](https://github.com/JannisConen/yesautomate-runtime) generated applications. Provides a FastAPI server that executes LLM-powered processes with cost tracking, execution logging, secret management, and file handling.

> Completely vibe-coded in 1 hour.

## Installation

```bash
pip install yesautomate-runtime
```

With optional extras:

```bash
# Langfuse observability
pip install yesautomate-runtime[langfuse]

# Azure Key Vault secrets
pip install yesautomate-runtime[azure]

# AWS Secrets Manager
pip install yesautomate-runtime[aws]

# Everything
pip install yesautomate-runtime[all]
```

## Quick start

### 1. Create a config file

Create `plsautomate.config.yaml` in your project root:

```yaml
project:
  id: my-project

llm:
  model: anthropic/claude-sonnet-4-6

processes:
  classify:
    process_id: classify
    trigger:
      type: webhook
    instructions: |
      Classify the incoming support ticket into one of: billing, technical, general.
```

### 2. Set your API keys

```bash
export ENDPOINT_API_KEYS=my-secret-key
export ANTHROPIC_API_KEY=sk-ant-...
```

`ENDPOINT_API_KEYS` is a comma-separated list of keys that authenticate requests to the runtime.

### 3. Start the server

```bash
yesautomate-runtime start --config plsautomate.config.yaml
```

The server starts on `http://localhost:8000` by default.

### 4. Call a process

```bash
curl -X POST http://localhost:8000/process/classify \
  -H "Authorization: Bearer my-secret-key" \
  -H "Content-Type: application/json" \
  -d '{"ticket_text": "I cannot log in to my account"}'
```

## API endpoints

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/health` | Health check with version and uptime |
| `POST` | `/process/<name>` | Execute a process (authenticated) |
| `GET` | `/executions` | Query execution history |
| `GET` | `/executions/:id` | Single execution detail |
| `GET` | `/executions/stats` | Aggregated cost, latency, and volume stats |

## Configuration

The runtime is configured via `plsautomate.config.yaml`. Environment variables can be referenced with `${VAR_NAME}` syntax.

### Top-level sections

```yaml
project:
  id: my-project              # Required: project identifier
  plsautomate_url: https://...   # Optional: PlsAutomate server URL
  version: 1.0.0

llm:
  model: anthropic/claude-sonnet-4-6  # Default LLM model (LiteLLM format)

database:
  url: sqlite+aiosqlite:///./data/app.db  # Execution log database

storage:
  type: local          # local or s3
  path: ./data/files   # Local file storage path

auth:
  methods:
    - type: api_key    # api_key or oauth2

secrets:
  provider: env        # env, plsautomate, azure_keyvault, aws_secrets_manager, sap_credential_store

observability:
  langfuse:
    enabled: false
    host: https://cloud.langfuse.com
```

### Process configuration

Each process defines an LLM-powered task:

```yaml
processes:
  my-process:
    process_id: my-process
    trigger:
      type: webhook           # webhook, schedule, or process
      cron: "0 9 * * *"       # For schedule triggers
      after: other-process    # For process triggers (chaining)
    instructions: |
      Your LLM instructions here...
    llm_model: openai/gpt-4o  # Override default model
    connector: exchange        # Optional: built-in connector
    review:
      enabled: true
      timeout: 24h
```

### Custom process modules

For advanced logic, provide Python modules for the three-phase pipeline:

```yaml
processes:
  my-process:
    process_id: my-process
    trigger:
      type: webhook
    before_module: processes.my_process.before     # Pre-processing
    execution_module: processes.my_process.execution  # Custom LLM call
    after_module: processes.my_process.after        # Post-processing
```

Each module should define an async function matching the phase signature. See the [PlsAutomate docs](https://github.com/JannisConen/yesautomate-runtime) for details.

## Secret providers

The runtime supports multiple secret backends:

| Provider | Config value | Description |
|----------|-------------|-------------|
| Environment | `env` | Read from environment variables (default) |
| PlsAutomate | `plsautomate` | Fetch from PlsAutomate server |
| Azure Key Vault | `azure_keyvault` | Azure Key Vault (`pip install yesautomate-runtime[azure]`) |
| AWS Secrets Manager | `aws_secrets_manager` | AWS Secrets Manager (`pip install yesautomate-runtime[aws]`) |
| SAP Credential Store | `sap_credential_store` | SAP BTP Credential Store |

## CLI reference

```
yesautomate-runtime start [OPTIONS]

Options:
  --config PATH   Path to plsautomate.config.yaml (default: plsautomate.config.yaml)
  --host TEXT      Bind host (default: 0.0.0.0)
  --port INT       Bind port (default: 8000)
```

## Development

```bash
# Clone and install
git clone https://github.com/JannisConen/yesautomate-runtime.git
cd yesautomate-runtime
pip install -e ".[dev]"

# Run tests
pytest -v

# Lint
ruff check .
```

## License

MIT
