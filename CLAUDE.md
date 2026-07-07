# low6_dbt_azureeastus2

Snowflake region: Azure East US 2. Shared conventions are in `~/.claude/CLAUDE.md`.

## Stack
- dbt Core
- Snowflake
- Conda env: dbt311
- dbt executable: /c/Users/WillBreeden/anaconda3/envs/dbt311/Scripts/dbt

## Project Structure
- models/{domain}/staging/ — raw source cleaning, prefixed stg_
- models/{domain}/intermediates/ — business logic, prefixed int_
- models/{domain}/marts/ — final consumption layer

## Project Variables
- `local_timezone: 'America/New_York'`

## Game Domains

Active domains (from `dbt_project.yml`, no `+enabled: false`):

| Domain | Client | game_type |
|---|---|---|
| bet365_overunder | Bet365 | pickem |
| bet99_picks | Bet99 | pickem |
| betway_picks | Betway | pickem |
| elf_collectyourelf | ELF | pickem (no entries/contest concept — challenge/product progress tracking; excluded from `low6_reporting`) |
| saracen | Saracen | pickem, bracket |
| bet99_bracket | Bet99 | bracket |
| bet365_uf | Bet365 | fantasy |
| cfl_fantasy | CFL | fantasy |
| opap_spintowin | OPAP | spin_to_win |
| gana_gamezone | Gana | pickem (predictor), streak (survivor), bracket (bracket) |

Archived domains (`+enabled: false`, under `models/_archive/`):

| Domain | Client | game_type |
|---|---|---|
| fanstake_rivals | Fanstake | pickem |
| oilers_picks | Oilers | pickem |
| pln_arcade | PLN | pickem |
| sackings_picks | Sackings | pickem |
| penn | Penn | squads |

## Supporting Domains
- `low6_reporting` — Cross-domain reporting, unions all active domains except `elf_collectyourelf`
