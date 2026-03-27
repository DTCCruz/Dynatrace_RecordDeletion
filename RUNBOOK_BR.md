# Guia do Usuário: Exportação e Limpeza no Grail

`grail_query_to_csv.py` exporta logs do Dynatrace Grail para CSV e também pode excluir os mesmos registros do Grail.

Use este guia como manual operacional: o que configurar, o que executar, o que esperar e como se recuperar com segurança.

> **AVISO IMPORTANTE - COMUNICADO LEGAL**
>
> - Este script foi desenvolvido exclusivamente para auxiliar **{{CUSTOMER_NAME}}** nos fluxos de exportação e exclusão de dados no Grail.
> - Este script é fornecido **AS IS**, como um **PONTO DE PARTIDA EM MELHOR ESFORÇO**, e **NÃO É UM PRODUTO DYNATRACE**.
> - A Dynatrace não oferece **NENHUMA GARANTIA**, **NENHUM SUPORTE OFICIAL**, nem **COMPROMISSO DE VERSIONAMENTO OU MANUTENÇÃO** para este script.
> - A exclusão de registros é uma capacidade existente das APIs da Dynatrace. Este script apenas demonstra uma possível abordagem para controle de dados no Grail, gerenciado pelo próprio cliente.
> - **{{CUSTOMER_NAME}}** reconhece responsabilidade por instalação, customização, implementação, validação, revisão de segurança e operação contínua, sem **COMPROMISSO DE SUPORTE FUTURO** da Dynatrace.

---

> **AVISO DE CUSTOS**
>
> - O uso desta ferramenta para consultar e excluir dados no Dynatrace Grail **pode incorrer em custos adicionais** baseados no seu modelo de consumo Dynatrace.
> - Os custos são calculados com base no volume de dados **consultados** (medido em GiB), não no volume de dados excluídos.
> - Antes de confirmar qualquer operação de exclusão, revise o **custo estimado da consulta** que o script exibe. Esta estimativa é baseada em bytes consultados e sua taxa de cobrança configurada.
> - Após a conclusão bem-sucedida da exclusão, o script consulta `dt.system.events` para relatar **custos de cobrança reais** para fins de auditoria e comparação.
> - Configure a taxa de cobrança no arquivo `.env`: `DT_LOG_QUERY_COST_RATE_PER_GIB=<sua_taxa>` (ex: `0.25` por GiB consultado na moeda escolhida).
> - **Verifique se sua taxa de cobrança corresponde ao seu contrato Dynatrace** antes de executar fluxos de trabalho de exclusão.

---

## Índice

1. [O que a ferramenta faz](#1-o-que-a-ferramenta-faz)
2. [Antes de começar](#2-antes-de-começar)
3. [Início rápido (primeira execução recomendada)](#3-início-rápido-primeira-execução-recomendada)
4. [Configuração (.env)](#4-configuração-env)
5. [Procedimentos operacionais passo a passo](#5-procedimentos-operacionais-passo-a-passo)
6. [Regras de segurança (leia antes de excluir)](#6-regras-de-segurança-leia-antes-de-excluir)
7. [Arquivos de saída e validação](#7-arquivos-de-saída-e-validação)
8. [Limpeza de longo período (múltiplos dias ou múltiplos meses)](#8-limpeza-de-longo-período-múltiplos-dias-ou-múltiplos-meses)
9. [Mensagens de console e significado](#9-mensagens-de-console-e-significado)
10. [Solução de problemas](#10-solução-de-problemas)
11. [Referência de API](#11-referência-de-api)

---

## 1. O que a ferramenta faz

Este script tem dois modos:

1. Modo de exportação: executa uma consulta DQL e grava os registros encontrados em um CSV com timestamp.
2. Modo de limpeza: após a exportação, envia requisições de exclusão definitiva no Grail para os registros correspondentes.

Uso seguro típico:

1. Execute primeiro somente a exportação.
2. Verifique o CSV.
3. Ative a limpeza apenas depois de validar o resultado da exportação.

---

## 2. Antes de começar

### 2.1 Requisitos

| Requisito | Detalhes |
| --- | --- |
| Python | 3.9+ (testado em 3.13) |
| Pacote Python | `requests` |
| Escopos do token Dynatrace | `storage:logs:read` e `storage:records:delete` |
| Diretório de execução | Pasta que contém `.env` e `grail_query_to_csv.py` |

### 2.2 Configuração inicial

```text
cd /path/to/buckets
python3 -m venv .venv
.venv/bin/pip install requests
```

Sempre execute com `.venv/bin/python` para manter as dependências consistentes.

---

## 3. Início rápido (primeira execução recomendada)

Siga exatamente esta sequência para uma primeira execução segura.

### Passo 1: Crie o `.env` a partir do `env.txt`

Use o `env.txt` como template inicial e crie um arquivo `.env` na mesma pasta.

```text
cp env.txt .env
```

Depois edite o `.env` e defina `DT_ENVIRONMENT`, `DT_TOKEN`, `DT_QUERY`, `DT_FROM`, `DT_TO` e `DT_OUT`.

### Passo 2: Valide configuração e permissões

```text
./.venv/bin/python grail_query_to_csv.py --validate-config
```

### Passo 3: Execute somente exportação

```text
./.venv/bin/python grail_query_to_csv.py
```

### Passo 4: Verifique o CSV de saída

Confirme:

1. O arquivo foi criado.
2. Timestamps e colunas estão corretos.
3. O volume de registros está dentro do esperado.

### Passo 5: Ative a limpeza somente se necessário

Use uma das opções:

1. `DT_CLEANUP=true` no `.env`, ou
2. `--cleanup` na linha de comando.

### Passo 6: Execute a limpeza

```text
./.venv/bin/python grail_query_to_csv.py --cleanup
```

Comando opcional de segurança antes da limpeza:

```text
./.venv/bin/python grail_query_to_csv.py --cleanup --dry-run-delete
```

---

## 4. Configuração (.env)

Use o `env.txt` como template inicial para criar o `.env`. Todas as configurações são lidas do `.env`. Variáveis de ambiente do shell sobrescrevem valores do `.env`.

### 4.1 Exemplo completo

```text
# Obrigatório
DT_ENVIRONMENT=https://<tenant-id>.apps.dynatrace.com
DT_TOKEN=dt0s16.<token-value>

# Consulta de exportação (DQL)
DT_QUERY=fetch logs | filter matchesValue(id, "your-filter")

# Consulta de exclusão (DQL)
# Se não for definida, o script usa DT_QUERY
DT_DELETE_QUERY=fetch logs | filter matchesValue(id, "your-filter")

# Janela de exportação
DT_FROM=2026-03-08T00:00:00.000000000Z
DT_TO=2026-03-09T00:00:00.000000000Z

# Janela de exclusão (opcional)
# Se omitida, o script usa DT_FROM/DT_TO
DT_DELETE_FROM=2026-03-01T00:00:00.000000000Z
DT_DELETE_TO=2026-03-09T00:00:00.000000000Z

# Nome base do CSV de saída
DT_OUT=grail_logs.csv

# Modo de limpeza (opcional)
DT_CLEANUP=true

# Fuso horário do timestamp no nome do arquivo
DT_TIMEZONE=America/Sao_Paulo

# Ajuste da validação pós-exclusão
DT_DELETE_VALIDATE_RETRIES=12
DT_DELETE_VALIDATE_INTERVAL_SECONDS=10
```

### 4.2 Guia das variáveis

| Variável | Obrigatória | Finalidade |
| --- | --- | --- |
| `DT_ENVIRONMENT` | Sim | URL do tenant |
| `DT_TOKEN` | Sim | Token de API |
| `DT_QUERY` | Sim | Consulta de seleção para exportação |
| `DT_DELETE_QUERY` | Não | Consulta de seleção para limpeza |
| `DT_FROM`, `DT_TO` | Sim | Janela de tempo da exportação |
| `DT_DELETE_FROM`, `DT_DELETE_TO` | Não | Janela de tempo da limpeza |
| `DT_OUT` | Sim | Nome base do arquivo CSV |
| `DT_CLEANUP` | Não | Ativa limpeza sem `--cleanup` |
| `DT_TIMEZONE` | Não | Fuso horário usado no nome do arquivo |
| `DT_DELETE_VALIDATE_RETRIES` | Não | Tentativas da verificação pós-exclusão |
| `DT_DELETE_VALIDATE_INTERVAL_SECONDS` | Não | Intervalo entre tentativas de validação |

### 4.3 Formato de timestamp

Todos os valores de tempo devem usar RFC3339 UTC com nanossegundos:

```text
YYYY-MM-DDTHH:MM:SS.000000000Z
```

Exemplo: `2026-03-08T00:00:00.000000000Z`

---

## 5. Procedimentos operacionais passo a passo

### 5.1 Somente exportação (base segura)

Use quando estiver validando uma consulta ou coletando dados sem excluir registros.

```text
./.venv/bin/python grail_query_to_csv.py
```

Resultado esperado:

1. A consulta é executada.
2. O arquivo CSV é criado com sufixo de timestamp.
3. Os dados remotos permanecem no Grail.

### 5.2 Exportação e limpeza (mesma janela)

Use quando as janelas de exportação e exclusão devem ser iguais.

```text
./.venv/bin/python grail_query_to_csv.py --cleanup
```

Resultado esperado:

1. A exportação executa primeiro.
2. O script valida as condições de segurança da exclusão.
3. Os registros correspondentes são excluídos em blocos de 24 horas.
4. A validação pós-exclusão verifica se ainda restaram registros.

### 5.3 Exportar uma janela e limpar outra

Se a janela de exclusão for maior que a janela de exportação, o script mostra aviso e exige confirmação explícita.

Use essa opção somente quando for intencional.

### 5.4 Sobrescrever valores via CLI

```text
--environment   URL ou ID do tenant
--token         Token Bearer
--query         Consulta DQL de exportação
--delete-query  Consulta DQL de exclusão (não deve conter limit)
--from          Início da exportação
--to            Fim da exportação
--delete-from   Início da exclusão
--delete-to     Fim da exclusão
--out           Caminho do CSV de saída
--cleanup       Ativa exclusão definitiva
```

Exemplo:

```text
./.venv/bin/python grail_query_to_csv.py \
  --from 2026-01-01T00:00:00.000000000Z \
  --to   2026-01-02T00:00:00.000000000Z
```

### 5.5 Limpar overrides antigos do shell

Se valores de execuções anteriores ainda estiverem ativos no shell, limpe-os:

```text
unset DT_FROM DT_TO DT_DELETE_FROM DT_DELETE_TO DT_QUERY DT_DELETE_QUERY
```

---

## 6. Regras de segurança (leia antes de excluir)

O script aplica múltiplas proteções para reduzir risco de perda acidental de dados.

### 6.1 O fim da janela de exclusão deve estar pelo menos 4 horas no passado

Se estiver muito recente, a limpeza é ignorada.

### 6.2 A consulta de exclusão não deve conter `| limit`

Se existir, a limpeza é ignorada para evitar exclusão parcial.

### 6.3 Aviso de janela divergente

Se a janela de limpeza ultrapassar a janela de exportação, o script exibe aviso e pede confirmação.

### 6.4 Verificação de existência antes da exclusão

O script verifica se ainda existem registros correspondentes antes de chamar as APIs de exclusão.

Se não houver registros, a limpeza para com segurança.

### 6.5 Validação pós-exclusão

Após concluir todos os blocos, o script consulta novamente com `| limit 1` por até `DT_DELETE_VALIDATE_RETRIES` tentativas.

---

## 7. Arquivos de saída e validação

Cada execução cria um novo CSV com timestamp.

Sequência de exemplo:

```text
grail_logs.csv
grail_logs_20260315_143012.csv
grail_logs_20260316_090511.csv
grail_logs_20260319_174822.csv
```

Checklist de validação:

1. O arquivo existe e não está vazio.
2. A contagem de linhas é plausível.
3. Campos/colunas correspondem ao esquema esperado.
4. O intervalo de tempo no CSV corresponde à janela selecionada.
5. O tamanho reportado de payload/download está dentro do limite esperado.

Observação: objetos JSON aninhados e arrays são serializados como strings JSON nas células do CSV.

### 7.1 Validação de integridade do pacote (SHA256 do MANIFEST)

Se você receber um pacote customer-ready com `MANIFEST.txt`, pode validar a integridade dos arquivos antes de usar.

1. Abra o diretório do pacote onde está o `MANIFEST.txt`.
2. Recalcule o SHA256 dos arquivos entregues.
3. Compare cada hash calculado com o valor correspondente na seção `SHA256` do `MANIFEST.txt`.
4. Se algum hash for diferente, o arquivo foi alterado (ou corrompido) após a geração do pacote.

```text
cd customer-ready-customer-name-YYYYMMDD
shasum -a 256 RUNBOOK.md RUNBOOK_BR.md RUNBOOK_ES.md grail_query_to_csv.py env.txt
```

---

## 8. Limpeza de longo período (múltiplos dias ou múltiplos meses)

A API de exclusão aceita no máximo 24 horas por chamada.

Para períodos longos, o script divide automaticamente em blocos de 24 horas e executa em sequência.

Padrão operacional recomendado:

1. Exporte primeiro uma amostra curta (1-2 dias).
2. Valide se a consulta está correta.
3. Amplie `DT_DELETE_FROM` e `DT_DELETE_TO` para o intervalo alvo completo.
4. Execute com limpeza habilitada.
5. Acompanhe o progresso bloco a bloco.

Se interromper (`Ctrl+C`), execute novamente o comando. Blocos já concluídos são seguros para repetição.

Comando para retomar:

```text
./.venv/bin/python grail_query_to_csv.py
```

---

## 9. Mensagens de console e significado

| Mensagem | Significado |
| --- | --- |
| `Running grail query from ... to ...` | Exportação iniciada |
| `Got N records (~X bytes payload, Y); writing CSV ...` | Registros retornados inline com estimativa de tamanho do payload |
| `No in-memory records in query result; downloading from query:download endpoint` | Resultado grande com download em streaming |
| `CSV written: X bytes (Y)` | Exportação concluída com tamanho final do CSV |
| `Downloaded CSV to ... (X bytes, Y)` | Download em streaming concluído com tamanho transferido |
| `WARNING: Deletion window extends beyond export window` | Janela de exclusão maior que a de exportação |
| `Will delete in N chunks of 24 hours each` | Plano de limpeza exibido |
| `[1/3] Deleting chunk 1...` | Bloco em execução |
| `[1/3] Chunk 1 deleted successfully.` | Bloco concluído |
| `No matching records found - nothing to delete.` | Não há mais registros para excluir |
| `Post-delete validation passed on attempt N` | Nenhum registro correspondente encontrado |
| `Remote Grail data kept (not deleted).` | Modo limpeza não habilitado |

### 9.1 Exemplos completos de console (contagem, bytes e aviso)

Exemplo A: registros inline com estimativa de bytes do payload e tamanho final do CSV.

```text
python3 grail_query_to_csv.py --cleanup
Running grail query from 2026-03-02T00:00:00.000000Z to 2026-03-03T00:00:00.000000Z
Run #5 (previous run files already exist for this base name)
Got 2,160 records (~1,555,200 bytes payload, 1.48 MB); writing CSV grail_logs_20260320_103910.csv
CSV written: 840,506 bytes (820.81 KB)
```

Exemplo B: janela de exportação menor que a janela de exclusão (aviso + confirmação).

```text
python3 grail_query_to_csv.py --cleanup
Running grail query from 2026-03-02T00:00:00.000000Z to 2026-03-03T00:00:00.000000Z
Run #5 (previous run files already exist for this base name)
Got 2,160 records (~1,555,200 bytes payload, 1.48 MB); writing CSV grail_logs_20260320_103910.csv
CSV written: 840,506 bytes (820.81 KB)

⚠️  WARNING: Deletion window extends beyond export window!
  Export window: 2026-03-02T00:00:00.000000Z to 2026-03-03T00:00:00.000000Z
  Delete window: 2026-03-01T00:00:00.000000Z to 2026-03-03T00:00:00.000000Z
  ❌ Deleting data BEFORE export start: 2026-03-01T00:00:00.000000Z < 2026-03-02T00:00:00.000000Z
  You will delete records that were never downloaded to CSV!
Proceed anyway? Type 'yes' to continue:
```

Exemplo C: sem registros inline; download do CSV em streaming com total de bytes transferidos.

```text
python3 grail_query_to_csv.py
Running grail query from 2026-03-01T00:00:00.000000Z to 2026-03-02T00:00:00.000000Z
No in-memory records in query result; downloading from query:download endpoint
Downloaded CSV to grail_logs_20260320_110001.csv (145,331,002 bytes, 138.60 MB)
```

### 9.2 Checklist de decisão para o aviso: janela de exclusão divergente

Use este fluxo quando aparecer:
`WARNING: Deletion window extends beyond export window!`

1. Compare a janela de exportação e a janela de exclusão mostradas no console.
2. Se a janela de exclusão for maior que a de exportação, não digite `yes` ainda.
3. Amplie a janela de exportação (`DT_FROM`/`DT_TO`) para cobrir todo o período que pretende excluir e exporte novamente.
4. Ou reduza a janela de exclusão (`DT_DELETE_FROM`/`DT_DELETE_TO`) para ficar totalmente dentro do período já exportado.
5. Reexecute a exportação e confirme se contagem de registros e tamanho em bytes estão dentro do esperado.
6. Execute o cleanup novamente e digite `yes` somente quando a janela de exclusão estiver totalmente alinhada aos dados exportados.

---

## 10. Solução de problemas

### 10.1 `ModuleNotFoundError: No module named 'requests'`

Causa: interpretador Python incorreto.

Correção:

```text
./.venv/bin/python grail_query_to_csv.py
```

Ou instale o pacote:

```text
python3 -m pip install requests
```

### 10.2 `0 records returned` mas existem registros no notebook do Grail

Verifique:

1. Janela UTC exata em `DT_FROM` e `DT_TO`.
2. Diferenças de conversão de fuso horário local.
3. Overrides antigos no shell.

Limpe os overrides se necessário:

```text
unset DT_FROM DT_TO
```

### 10.3 `Cleanup skipped: deletion end time must be at least 4 hours in the past`

Defina `DT_DELETE_TO` para pelo menos 4 horas antes do horário atual.

### 10.4 `delete execute status 400: exceeds maximum duration`

Causa: a janela da chamada de exclusão excedeu 24 horas.

Correção: use limites RFC3339 exatos com `.000000000Z`. O chunking do script foi projetado para reforçar essa regra.

### 10.5 `Transient delete status polling error (N/5)`

Causa: falha temporária de rede durante o polling de status.

Comportamento: o script tenta novamente automaticamente até 5 vezes.

### 10.6 Validação pós-exclusão não conclui com sucesso

A replicação do Grail pode ter atraso.

Aumente tentativas e intervalo de validação:

```text
DT_DELETE_VALIDATE_RETRIES=30
DT_DELETE_VALIDATE_INTERVAL_SECONDS=30
```

---

## 11. Referência de API

Referência Swagger:
Use seu próprio ID de tenant para validação e verificação: `https://YOUR_TENANT_ID.apps.dynatrace.com/platform/swagger-ui/index.html?urls.primaryName=Grail+-+Storage+Record+Deletion`

| Operação | Endpoint | Método |
| --- | --- | --- |
| Execute query | `/platform/storage/query/v2/query:execute` | POST |
| Poll query | `/platform/storage/query/v2/query:poll` | GET |
| Download result | `/platform/storage/query/v1/query:download` | GET |
| Execute delete | `/platform/storage/record/v1/delete:execute` | POST |
| Poll delete status | `/platform/storage/record/v1/delete:status` | POST |

Notas da API de consulta:

1. O script tenta v2 primeiro e depois faz fallback para v1.
2. Máximo de registros por resultado: 50.000.000.
3. Intervalo de polling: 2 segundos.

Notas da API de exclusão:

1. Retorna HTTP 202 e `taskId` imediatamente.
2. A janela máxima por chamada de exclusão é de 24 horas.
3. O fim da janela de exclusão deve estar pelo menos 4 horas no passado.
4. Valores de tempo devem usar UTC com sufixo `Z`.
