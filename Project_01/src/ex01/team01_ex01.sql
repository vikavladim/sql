select coalesce("user".name,'not defined') as name ,
coalesce("user".lastname,'not defined') as lastname,
currency.name as currency_name,
money*rate_to_usd as currency_in_usd from balance
left join "user" on balance.user_id="user".id
join currency on currency_id=currency.id
join
(select user_id, balance.currency_id,balance.updated, min(abs(extract(epoch from (currency.updated-balance.updated)))) from currency
join balance on currency.id=balance.currency_id
group by user_id, currency_id,balance.updated) as t on
(abs(extract(epoch from (currency.updated-balance.updated))))=t.min
and t.user_id=balance.user_id and balance.currency_id=t.currency_id
order by name desc, lastname, currency_name;