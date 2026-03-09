# Oktto Pipeline

Pipeline ETL para consumir a API da Oktto, tratar dados comerciais e publicar no Google Sheets.

## Estrutura

- `src/clients`: clientes de API externa (Oktto e Google Sheets)
- `src/extract`: extração por domínio
- `src/transform`: normalização e views de negocio
- `src/load`: escrita de dados no destino
- `src/jobs`: orquestrações por etapa
- `src/utils`: logger, retry e datas
- `tests`: testes unitarios

## Setup

1. Crie e ative um ambiente virtual Python.
2. Instale dependencias:

```bash
pip install -r requirements.txt
```

3. Copie `.env.example` para `.env` e preencha os valores.

## Execucao

```bash
python -m src.main --job sync_dimensions
python -m src.main --job sync_leads
python -m src.main --job sync_sales
python -m src.main --job sync_full
```

## Interface visual simples

Depois de instalar as dependencias, rode:

```bash
streamlit run src/ui/app.py
```

Na tela voce consegue:

- salvar token e configuracoes no `.env`
- testar conexao da Oktto API
- testar conexao com Google Sheets
- executar jobs (`sync_dimensions`, `sync_leads`, `sync_sales`, `sync_full`)

## Usando direto no GitHub

### 1. Subir o projeto

```bash
git init
git add .
git commit -m "chore: inicializa pipeline oktto"
git branch -M main
git remote add origin <url-do-seu-repo>
git push -u origin main
```

### 2. Configurar Secrets no GitHub

No repositorio, abra `Settings > Secrets and variables > Actions > New repository secret` e crie:

- `OKTTO_API_BASE_URL`
- `OKTTO_API_TOKEN`
- `OKTTO_TIMEOUT_SECONDS` (ex: `30`)
- `OKTTO_MAX_RETRIES` (ex: `3`)
- `OKTTO_BACKOFF_FACTOR` (ex: `0.5`)
- `OKTTO_PAGE_SIZE` (ex: `100`)
- `GOOGLE_SHEETS_SPREADSHEET_ID`
- `GOOGLE_SHEETS_CREDENTIALS_JSON_CONTENT` (conteudo completo do JSON da service account)
- `LOG_LEVEL` (ex: `INFO`)

### 3. Rodar no GitHub Actions

- Workflow de testes: `.github/workflows/ci.yml`
- Workflow de execucao ETL: `.github/workflows/run_pipeline.yml`

Para rodar manualmente:

1. Abra `Actions`
2. Selecione `Run Oktto Pipeline`
3. Clique em `Run workflow`
4. Escolha o job (`sync_dimensions`, `sync_leads`, `sync_sales`, `sync_full`)

Esse workflow tambem roda agendado diariamente (09:00 UTC) com `sync_full`.

## Como usar a interface visual "la"

GitHub sozinho nao hospeda tela Streamlit para uso continuo. Para usar a interface visual online, conecte o repositorio em um host de app Python, por exemplo:

- Streamlit Community Cloud
- Render
- Railway

No Streamlit Community Cloud, use:

- Repository: seu repo no GitHub
- Branch: `main`
- Main file path: `src/ui/app.py`

E configure os mesmos valores como secrets da plataforma.

## Modo publico para todos usarem

Se voce quer deixar aberto para qualquer pessoa usar com o proprio token da Oktto e gravar no proprio Google Sheets:

1. No host do app (ex: Streamlit Cloud), defina `APP_PUBLIC_MODE=true`.
2. Configure variaveis do OAuth Google no ambiente do app:
   - `GOOGLE_OAUTH_CLIENT_ID`
   - `GOOGLE_OAUTH_CLIENT_SECRET`
   - `GOOGLE_OAUTH_REDIRECT_URI` (URL publica do app)
3. No Google Cloud Console, adicione o redirect URI no OAuth Client.
4. O usuario final faz login Google na tela, informa token Oktto e Spreadsheet ID, escolhe o job e executa.

No modo publico, a tela oferece:

- envio para Google Sheets via OAuth do proprio usuario
- extracao e download de CSV

Observacoes importantes para uso publico:

- O token Oktto nao e salvo no `.env`.
- As credenciais Google ficam apenas na sessao atual da interface.
- Limites de API e custos ficam por conta da conta Oktto de quem fornecer o token.
- Se quiser controle de abuso, coloque autenticacao na frente do app (por exemplo, Cloudflare Access).

## Abas sugeridas no Sheets

- `raw_leads`, `raw_sales`
- `raw_users`, `raw_teams`, `raw_products`
- `raw_funnels`, `raw_stages`, `raw_additional_fields`
- `leads_tratados`, `sales_tratadas`, `painel_comercial`
