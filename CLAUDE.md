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

| Domain | Client | game_type |
|---|---|---|
| bet365_overunder | Bet365 | pickem |
| bet99_picks | Bet99 | pickem |
| betway_picks | Betway | pickem |
| elf_collectyourelf | ELF | pickem |
| fanstake_rivals | Fanstake | pickem |
| oilers_picks | Oilers | pickem |
| pln_arcade | PLN | pickem |
| sackings_picks | Sackings | pickem |
| saracen | Saracen | pickem |

## Supporting Domains
- `low6_reporting` — Cross-domain reporting
