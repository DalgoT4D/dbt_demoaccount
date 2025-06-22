{{ config(
    materialized = 'table'
) }}

with raw as (

    select * 
    from {{ source('source_csv', 'fifa21_raw_data') }}

),

renamed as (

    select
        cast("ID" as integer) as player_id,
        trim("Name") as short_name,
        trim("LongName") as full_name,
        trim("photoUrl") as photo_url,
        trim("playerUrl") as profile_url,
        trim("Nationality") as nationality,
        trim("Positions") as positions,
        nullif(trim("Age"), '')::int as age,
        nullif(trim("↓OVA"), '')::int as overall_rating,
        nullif(trim("POT"), '')::int as potential,
        trim("Team & Contract") as team_contract,
        trim("foot") as preferred_foot,

        -- Handle height like "6'1""" → convert to inches
        cast(
            split_part(split_part("Height", '''', 1), '"', 1) as int
        ) * 12
        + cast(
            nullif(split_part(split_part("Height", '''', 2), '"', 1), '') as int
        ) as height_in_inches,

        -- Weight: strip "lbs" and cast
        replace("Weight", 'lbs', '')::int as weight_lbs,

        -- Value and Wage: remove € and convert K/M suffixes
        case
            when "Value" like '%M%' then round(cast(replace(replace("Value", '€', ''), 'M', '') as numeric) * 1000000)
            when "Value" like '%K%' then round(cast(replace(replace("Value", '€', ''), 'K', '') as numeric) * 1000)
            else null
        end as value_eur,

        case
            when "Wage" like '%M%' then round(cast(replace(replace("Wage", '€', ''), 'M', '') as numeric) * 1000000)
            when "Wage" like '%K%' then round(cast(replace(replace("Wage", '€', ''), 'K', '') as numeric) * 1000)
            else null
        end as wage_eur,

        -- Release clause
        case
            when "Release Clause" like '%M%' then round(cast(replace(replace("Release Clause", '€', ''), 'M', '') as numeric) * 1000000)
            when "Release Clause" like '%K%' then round(cast(replace(replace("Release Clause", '€', ''), 'K', '') as numeric) * 1000)
            else null
        end as release_clause_eur,

        -- Cast core skills
        nullif(trim("Dribbling"), '')::int as dribbling,
        nullif(trim("Finishing"), '')::int as finishing,
        nullif(trim("Short Passing"), '')::int as short_passing,
        nullif(trim("Ball Control"), '')::int as ball_control,
        nullif(trim("Reactions"), '')::int as reactions,
        nullif(trim("Stamina"), '')::int as stamina,
        nullif(trim("Strength"), '')::int as strength,
        nullif(trim("Composure"), '')::int as composure,

        -- Growth potential feature
        nullif(trim("POT"), '')::int - nullif(trim("↓OVA"), '')::int as growth_potential

    from raw

),

deduplicated as (

    select *
    from (
        select *, row_number() over (partition by player_id order by overall_rating desc) as row_num
        from renamed
    ) as dedup_source
    where row_num = 1

)

select * from deduplicated