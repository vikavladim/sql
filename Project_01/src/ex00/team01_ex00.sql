with
tt(user_id, type, currency_id,volume) as
(select user_id, type, currency_id,sum(money) from balance
group by user_id, type, currency_id),

t(id,max) as
(select currency.id, max(currency.updated) from currency
group by currency.id)

select distinct coalesce("user".name,'not defined') as name,
coalesce("user".lastname,'not defined') as lastname,
balance.type as balance_type,
tt.volume as volume,
coalesce(currency.name,'not defined') as currency_name,
coalesce(rate_to_usd,1) as last_rate_to_usd,
tt.volume*coalesce(rate_to_usd,1) as total_volume_in_usd 
from balance
left join "user" on balance.user_id="user".id
left join t on balance.currency_id=t.id
left join currency on currency_id=currency.id and currency.updated=t.max
natural join tt
order by name desc, lastname, balance_type;